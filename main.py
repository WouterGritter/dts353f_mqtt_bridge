import os
import time
from typing import Optional

import minimalmodbus
import paho.mqtt.client as mqtt

from computed_fetcher import ComputedFetcher
from fetcher import Fetcher
from reader_fetcher import ReaderFetcher

DTS353F_USB_DEVICE = os.getenv('DTS353F_USB_DEVICE')
UPDATE_INTERVAL = float(os.getenv('UPDATE_INTERVAL', '0.5'))
MQTT_BROKER_ADDRESS = os.getenv('MQTT_BROKER_ADDRESS', 'localhost')
MQTT_BROKER_PORT = int(os.getenv('MQTT_BROKER_PORT', '1883'))
MQTT_TOPIC_FORMAT = os.getenv('MQTT_TOPIC_FORMAT', 'dts353f/{attribute}')
MQTT_QOS = int(os.getenv('MQTT_QOS', '0'))
MQTT_RETAIN = os.getenv('MQTT_RETAIN', 'true') == 'true'

ATTRIBUTE_FETCHERS: dict[str, Fetcher] = {
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
    'amperage/total': ComputedFetcher(['amperage/l1', 'amperage/l2', 'amperage/l3'], lambda v: sum([abs(n) for n in v])),
}

mqttc: Optional[mqtt.Client] = None
rs485: Optional[minimalmodbus.Instrument] = None


def publish_attribute(attribute: str, value: float):
    topic = MQTT_TOPIC_FORMAT.replace('{attribute}', attribute)
    mqttc.publish(topic, str(round(value, 2)), qos=MQTT_QOS, retain=MQTT_RETAIN)


def update_attributes():
    values = {}
    for attribute, fetcher in ATTRIBUTE_FETCHERS.items():
        value = fetcher.fetch(rs485, values)
        values[attribute] = value

    for attribute, value in values.items():
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
