from typing import Optional, Callable

import minimalmodbus

from fetcher import Fetcher


class ComputedFetcher(Fetcher):

    def __init__(self, in_attributes: list[str], expression: Callable[[list[float]], float]):
        self.in_attributes = in_attributes
        self.expression = expression

    def fetch(self, rs485: minimalmodbus.Instrument, previous_values: dict[str, float]) -> Optional[float]:
        values = [previous_values[attribute] for attribute in self.in_attributes]
        if None in values:
            return None

        return self.expression(values)