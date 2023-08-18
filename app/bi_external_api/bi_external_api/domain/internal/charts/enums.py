import enum


class OnOff(enum.Enum):
    on = "on"
    off = "off"


class ShowHide(enum.Enum):
    show = "show"
    hide = "hide"


class VisualizationId(enum.Enum):
    line = 'line'
    area = 'area'
    area100p = 'area100p'  #
    column = 'column'
    column100p = 'column100p'
    bar = 'bar'
    bar100p = 'bar100p'
    scatter = 'scatter'
    pie = 'pie'
    donut = 'donut'
    metric = 'metric'
    treemap = 'treemap'
    flatTable = 'flatTable'
    pivotTable = 'pivotTable'
    geolayer = 'geolayer'


class PlaceholderId(enum.Enum):
    """
    Value must be used during serialization
    """
    X = 'x'
    Y = 'y'
    Y2 = 'y2'
    FlatTableColumns = 'flat-table-columns'
    Geopoint = 'geopoint'
    Geopoligon = 'geopolygon'
    Measures = 'measures'
    Size = 'size'
    Points = 'points'
    Dimensions = 'dimensions'
    PivotTableColumns = 'pivot-table-columns'
    Rows = 'rows'
    Polyline = 'polyline'
    Grouping = 'grouping'
    Heatmap = 'heatmap'
    Sort = 'sort-container'


class DatasetFieldType(enum.Enum):
    DIMENSION = enum.auto()
    MEASURE = enum.auto()
    PSEUDO = enum.auto()


class SortDirection(enum.Enum):
    ASC = "ASC"
    DESC = "DESC"


class Operation(enum.Enum):
    """
    This enum is used in charts, filters & dash selectors
    Consider pull up to common UI layer
    """
    IN = 'IN'
    NIN = 'NIN'
    EQ = 'EQ'
    NE = 'NE'
    GT = 'GT'
    LT = 'LT'
    GTE = 'GTE'
    LTE = 'LTE'
    ISNULL = 'ISNULL'
    ISNOTNULL = 'ISNOTNULL'
    ISTARTSWITH = 'ISTARTSWITH'
    STARTSWITH = 'STARTSWITH'
    IENDSWITH = 'IENDSWITH'
    ENDSWITH = 'ENDSWITH'
    ICONTAINS = 'ICONTAINS'
    CONTAINS = 'CONTAINS'
    NOTICONTAINS = 'NOTICONTAINS'
    NOTCONTAINS = 'NOTCONTAINS'
    BETWEEN = 'BETWEEN'
    LENEQ = 'LENEQ'
    LENGT = 'LENGT'
    LENGTE = 'LENGTE'
    LENLT = 'LENLT'
    LENLTE = 'LENLTE'
