from typing import Sequence, Dict

from eventz.immutable_sequence import ImmutableSequence

from tests.example.child import Child


class Children(ImmutableSequence):
    def __init__(self, name: str, items: Sequence[Child]):
        super().__init__(items)
        self.name = name

    def get_json_data(self) -> Dict:
        return {
            "name": self.name,
            "items": self._items,
        }
