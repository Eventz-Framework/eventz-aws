from datetime import datetime

from eventz.entity import Entity
from eventz.messages import Event

from tests.example.child import Child
from tests.example.children import Children


class Parent(Entity):
    pass


class ParentCreated(Event):
    version: int = 1

    def __init__(
        self,
        parent_id: str,
        children: Children,
        uuid: str = None,
        timestamp: datetime = None,
    ):
        super().__init__(uuid, timestamp)
        self.parent_id: str = parent_id
        self.children: Children = children


class ChildChosen(Event):
    version: int = 1

    def __init__(
        self,
        parent_id: str,
        child: Child,
        uuid: str = None,
        timestamp: datetime = None,
    ):
        super().__init__(uuid, timestamp)
        self.parent_id: str = parent_id
        self.child: Child = child
