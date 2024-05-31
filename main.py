import os
import time
from abc import ABC, abstractmethod
from typing import Optional, Callable

import minimalmodbus
import paho.mqtt.client as mqtt


class Fetcher(ABC):
    @abstractmethod
    def fetch(self, rs485: minimalmodbus.Instrument) -> float:
        pass


class FloatFetcher(Fetcher):
    def __init__(self, address: int):
        self.address = address

    def fetch(self, rs485: minimalmodbus.Instrument) -> float:
        return rs485.read_float(self.address)


class ComputedFetcher(Fetcher):
    def __init__(self, fetchers: list[Fetcher], expression: Callable[[list[float]], float]):
        self.fetchers = fetchers
        self.expression = expression

    def fetch(self, rs485: minimalmodbus.Instrument) -> float:
        values = []
        for fetcher in self.fetchers:
            values.append(fetcher.fetch(rs485))

        return self.expression(values)


DTS353F_USB_DEVICE = os.getenv('DTS353F_USB_DEVICE')
UPDATE_INTERVAL = float(os.getenv('UPDATE_INTERVAL', '0.5'))
MQTT_BROKER_ADDRESS = os.getenv('MQTT_BROKER_ADDRESS', 'localhost')
MQTT_BROKER_PORT = int(os.getenv('MQTT_BROKER_PORT', '1883'))
MQTT_TOPIC_FORMAT = os.getenv('MQTT_TOPIC_PREFIX', 'dts353f/{attribute}')
MQTT_QOS = int(os.getenv('MQTT_QOS', '1'))
MQTT_RETAIN = os.getenv('MQTT_RETAIN', 'true') == 'true'

ATTRIBUTE_MAP = {
    'energy/delivery': FloatFetcher(0x0108),
    'energy/redelivery': FloatFetcher(0x0110),
    'energy/total': ComputedFetcher([FloatFetcher(0x0108), FloatFetcher(0x0110)], lambda values: values[0] - values[1]),
    'power/total': FloatFetcher(0x001C),
    'power/l1': FloatFetcher(0x001E),
    'power/l2': FloatFetcher(0x0020),
    'power/l3': FloatFetcher(0x0022),
    'voltage/l1': FloatFetcher(0x000E),
    'voltage/l2': FloatFetcher(0x0010),
    'voltage/l3': FloatFetcher(0x0012),
    'voltage/average': ComputedFetcher([FloatFetcher(0x000E), FloatFetcher(0x0010), FloatFetcher(0x0012)], lambda values: sum(values) / len(values)),
}

mqttc: Optional[mqtt.Client] = None
rs485: Optional[minimalmodbus.Instrument] = None


def update_attribute(attribute: str):
    fetcher = ATTRIBUTE_MAP[attribute]

    try:
        result = fetcher.fetch(rs485)
    except Exception as ex:
        print(f'Error reading attribute {attribute}.')
        print(ex)
        return

    result = str(round(result, 3))

    topic = MQTT_TOPIC_FORMAT.replace('{attribute}', attribute)
    mqttc.publish(topic, result, qos=MQTT_QOS, retain=MQTT_RETAIN)


def update_attributes():
    for attribute in ATTRIBUTE_MAP:
        update_attribute(attribute)


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
