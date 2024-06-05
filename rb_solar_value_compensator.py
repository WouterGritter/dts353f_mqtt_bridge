from typing import Optional

from value_compensator import ValueCompensator

import paho.mqtt.client as mqtt

SOLAR_PHASE = 'l3'

SOLAR_POWER_TOPIC = 'rb-solar/power'
P1_POWER_TOPIC = 'p1/power/{phase}'
P1_VOLTAGE_TOPIC = 'p1/voltage/{phase}'

PHASE_PHANTOM_POWER = 35
MIN_VOLTAGE_DROP = 2


class RbSolarValueCompensator(ValueCompensator):

    def __init__(self, mqtt_broker: str, mqtt_port: int):
        self.solar_power_topic = SOLAR_POWER_TOPIC
        self.p1_power_topic = P1_POWER_TOPIC.replace('{phase}', SOLAR_PHASE)
        self.p1_voltage_topic = P1_VOLTAGE_TOPIC.replace('{phase}', SOLAR_PHASE)

        self.solar_power: Optional[int] = None
        self.p1_phase_power: Optional[int] = None
        self.p1_phase_voltage: Optional[int] = None

        mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        mqttc.connect(mqtt_broker, mqtt_port, 60)
        mqttc.loop_start()

        mqttc.on_message = self._on_message

        mqttc.subscribe(self.solar_power_topic)
        mqttc.subscribe(self.p1_power_topic)
        mqttc.subscribe(self.p1_voltage_topic)

    def _on_message(self, client, userdata, message):
        if message.topic == self.solar_power_topic:
            self.solar_power = float(message.payload)
        elif message.topic == self.p1_power_topic:
            self.p1_phase_power = float(message.payload)
        elif message.topic == self.p1_voltage_topic:
            self.p1_phase_voltage = float(message.payload)

    def compensate(self, attributes: dict[str, float]):
        if self.solar_power is None or self.p1_phase_power is None or self.p1_phase_voltage is None:
            return

        local_phase_power = attributes[f'power/{SOLAR_PHASE}']
        local_phase_voltage = attributes[f'voltage/{SOLAR_PHASE}']

        redelivering = (self.solar_power > PHASE_PHANTOM_POWER
                        and (self.p1_phase_power < local_phase_power or local_phase_voltage - self.p1_phase_voltage >= MIN_VOLTAGE_DROP))

        if redelivering:
            attributes[f'power/{SOLAR_PHASE}'] *= -1
            attributes[f'amperage/{SOLAR_PHASE}'] *= -1
