import enum


class ControlType(enum.Enum):
    manual = "manual"
    dataset = "dataset"
    external = "external"


class TabItemType(enum.Enum):
    title = "title"
    text = "text"
    widget = "widget"
    control = "control"


class TextSize(enum.Enum):
    xs = "xs"
    s = "s"
    m = "m"
    l = "l"


class ControlSelectType(enum.Enum):
    select = "select"
    date = "date"
    input = "input"
