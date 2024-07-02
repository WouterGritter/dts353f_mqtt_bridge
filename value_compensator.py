from abc import ABC, abstractmethod


class ValueCompensator(ABC):
    @abstractmethod
    def compensate(self, attributes: dict[str, float]):
        pass
