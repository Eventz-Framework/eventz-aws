from eventz.value_object import ValueObject


class Child(ValueObject):
    """
    Name is the unique identifier. Two children with the same name are equal.
    """

    def __init__(self, name: str):
        self.name: str = name

    def __hash__(self):
        return hash((self.name,))
