from abc import ABC, abstractmethod
from typing import Optional

import minimalmodbus


class Fetcher(ABC):

    @abstractmethod
    def fetch(self, rs485: minimalmodbus.Instrument, previous_values: dict[str, float]) -> Optional[float]:
        pass
