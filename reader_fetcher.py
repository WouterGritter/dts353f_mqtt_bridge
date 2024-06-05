from typing import Optional, Callable

import minimalmodbus

from fetcher import Fetcher


class ReaderFetcher(Fetcher):

    def __init__(self, address: int, modifier: Optional[Callable[[float], float]] = None):
        self.address = address
        self.modifier = modifier

    def fetch(self, rs485: minimalmodbus.Instrument, previous_values: dict[str, float]) -> Optional[float]:
        try:
            value = rs485.read_float(self.address)
            if self.modifier is not None:
                value = self.modifier(value)

            return value
        except Exception as ex:
            print(f'Error reading address {self.address:X}.')
            print(ex)
            return None
