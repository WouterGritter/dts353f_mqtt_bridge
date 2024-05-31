import os
import time
from abc import ABC, abstractmethod
from typing import Optional, Callable

import minimalmodbus
import paho.mqtt.client as mqtt


class Fetcher(ABC):

    @abstractmethod
    def fetch(self, previous_values: dict[str, float]) -> Optional[float]:
        pass


class ReaderFetcher(Fetcher):

    def __init__(self, address: int, modifier: Optional[Callable[[float], float]] = None):
        self.address = address
        self.modifier = modifier

    def fetch(self, previous_values: dict[str, float]) -> Optional[float]:
        try:
            value = rs485.read_float(self.address)
            if self.modifier is not None:
                value = self.modifier(value)

            return value
        except Exception as ex:
            print(f'Error reading address {self.address:X}.')
            print(ex)
            return None


class ComputedFetcher(Fetcher):

    def __init__(self, in_attributes: list[str], expression: Callable[[list[float]], float]):
        self.in_attributes = in_attributes
        self.expression = expression

    def fetch(self, previous_values: dict[str, float]) -> Optional[float]:
        values = [previous_values[attribute] for attribute in self.in_attributes]
        if None in values:
            return None

        return self.expression(values)


DTS353F_USB_DEVICE = os.getenv('DTS353F_USB_DEVICE')
UPDATE_INTERVAL = float(os.getenv('UPDATE_INTERVAL', '0.5'))
MQTT_BROKER_ADDRESS = os.getenv('MQTT_BROKER_ADDRESS', 'localhost')
MQTT_BROKER_PORT = int(os.getenv('MQTT_BROKER_PORT', '1883'))
MQTT_TOPIC_FORMAT = os.getenv('MQTT_TOPIC_PREFIX', 'dts353f/{attribute}')
MQTT_QOS = int(os.getenv('MQTT_QOS', '1'))
MQTT_RETAIN = os.getenv('MQTT_RETAIN', 'true') == 'true'

ATTRIBUTE_FETCHERS = {
    'energy/delivery': ReaderFetcher(0x0108),
    'energy/redelivery': ReaderFetcher(0x0110),
    'energy/total': ComputedFetcher(['energy/delivery', 'energy/redelivery'], lambda v: v[0] - v[1]),
    'power/total': ReaderFetcher(0x001C, lambda p: p * 1000),
    'power/l1': ReaderFetcher(0x001E, lambda p: p * 1000),
    'power/l2': ReaderFetcher(0x0020, lambda p: p * 1000),
    'power/l3': ReaderFetcher(0x0022, lambda p: p * 1000),
    'voltage/l1': ReaderFetcher(0x000E),
    'voltage/l2': ReaderFetcher(0x0010),
    'voltage/l3': ReaderFetcher(0x0012),
    'voltage/average': ComputedFetcher(['voltage/l1', 'voltage/l2', 'voltage/l3'], lambda v: sum(v) / len(v)),
    'amperage/l1': ComputedFetcher(['power/l1', 'voltage/l1'], lambda v: v[0] / v[1]),
    'amperage/l2': ComputedFetcher(['power/l2', 'voltage/l2'], lambda v: v[0] / v[1]),
    'amperage/l3': ComputedFetcher(['power/l3', 'voltage/l3'], lambda v: v[0] / v[1]),
    'amperage/total': ComputedFetcher(['amperage/l1', 'amperage/l2', 'amperage/l3'], lambda v: sum(v)),
}


mqttc: Optional[mqtt.Client] = None
rs485: Optional[minimalmodbus.Instrument] = None


def publish_attribute(attribute: str, value: float):
    topic = MQTT_TOPIC_FORMAT.replace('{attribute}', attribute)
    mqttc.publish(topic, str(round(value, 2)), qos=MQTT_QOS, retain=MQTT_RETAIN)


def update_attributes():
    values = {}
    for attribute, fetcher in ATTRIBUTE_FETCHERS.items():
        value = fetcher.fetch(values)
        values[attribute] = value
        if value is not None:
            publish_attribute(attribute, value)


def main():
    global mqttc, rs485

    print(f'{DTS353F_USB_DEVICE=}')
    print(f'{UPDATE_INTERVAL=}')
    print(f'{MQTT_BROKER_ADDRESS=}')
    print(f'{MQTT_BROKER_PORT=}')
    print(f'{MQTT_TOPIC_FORMAT=}')
    print(f'{MQTT_QOS=}')
    print(f'{MQTT_RETAIN=}')

    mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqttc.connect(MQTT_BROKER_ADDRESS, MQTT_BROKER_PORT, 60)
    mqttc.loop_start()

    rs485 = minimalmodbus.Instrument(DTS353F_USB_DEVICE, 1)
    rs485.serial.baudrate = 9600
    rs485.serial.bytesize = 8
    rs485.serial.parity = minimalmodbus.serial.PARITY_EVEN
    rs485.serial.stopbits = 1
    rs485.serial.timeout = 0.5
    rs485.debug = False
    rs485.mode = minimalmodbus.MODE_RTU

    while True:
        time.sleep(UPDATE_INTERVAL)
        update_attributes()


if __name__ == '__main__':
    main()
