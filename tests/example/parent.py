from datetime import datetime
from typing import Optional

from eventz.entity import Entity
from eventz.messages import Event

from tests.example.child import Child
from tests.example.children import Children


class Parent(Entity):
    pass


class ParentCreated(Event):
    __version__: int = 1

    def __init__(
        self,
        aggregate_id: str,
        children: Children,
        __msgid__: Optional[str] = None,
        __timestamp__: Optional[datetime] = None,
        __seq__: Optional[int] = None,
    ):
        super().__init__(aggregate_id, __msgid__, __timestamp__, __seq__)
        self.children: Children = children


class ChildChosen(Event):
    __version__: int = 1

    def __init__(
        self,
        aggregate_id: str,
        child: Child,
        __msgid__: Optional[str] = None,
        __timestamp__: Optional[datetime] = None,
        __seq__: Optional[int] = None,
    ):
        super().__init__(aggregate_id, __msgid__, __timestamp__, __seq__)
        self.child: Child = child
