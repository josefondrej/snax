import enum


class ValueType(enum.Enum):
    UNKNOWN = 0
    STRING = 1
    INT = 2
    FLOAT = 3
    BOOL = 4
    TIMESTAMP = 5
    STRING_LIST = 6
    INT_LIST = 7
    FLOAT_LIST = 8
    BOOL_LIST = 9
    TIMESTAMP_LIST = 10
    NULL = 11

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name

    def __eq__(self, other):
        if not isinstance(other, ValueType):
            return False
        return self.name == other.name

    def __hash__(self):
        return hash(self.name)

    @staticmethod
    def from_string(value):
        try:
            return ValueType[value]
        except KeyError:
            return ValueType.UNKNOWN


Unknown = ValueType.UNKNOWN
String = ValueType.STRING
Int = ValueType.INT
Float = ValueType.FLOAT
Bool = ValueType.BOOL
Timestamp = ValueType.TIMESTAMP
StringList = ValueType.STRING_LIST
IntList = ValueType.INT_LIST
FloatList = ValueType.FLOAT_LIST
BoolList = ValueType.BOOL_LIST
TimestampList = ValueType.TIMESTAMP_LIST
Null = ValueType.NULL
