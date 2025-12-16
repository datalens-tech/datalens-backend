from dl_formula_ref.categories.hash import CATEGORY_HASH
from dl_formula_ref.localization import get_gettext
from dl_formula_ref.registry.base import FunctionDocRegistryItem
from dl_formula_ref.registry.example import SimpleExample


_ = get_gettext()


FUNCTION_MD5 = FunctionDocRegistryItem(
    name="md5",
    category=CATEGORY_HASH,
    description=_("Returns the MD5 hash of {arg:0} as a hexadecimal string."),
    examples=[
        SimpleExample('MD5("DataLens") = "C1FD5D9E4189FB89C1021A72F7E06C00"'),
    ],
)

FUNCTION_SHA1 = FunctionDocRegistryItem(
    name="sha1",
    category=CATEGORY_HASH,
    description=_("Returns the SHA-1 hash of {arg:0} as a hexadecimal string."),
    examples=[
        SimpleExample('SHA1("DataLens") = "F4EA6F8285E57FC18D8CA03672703B52C302231A"'),
    ],
)

FUNCTION_SHA256 = FunctionDocRegistryItem(
    name="sha256",
    category=CATEGORY_HASH,
    description=_("Returns the SHA-256 hash of {arg:0} as a hexadecimal string."),
    examples=[
        SimpleExample('SHA256("DataLens") = "7466C4153CEB3184D0FDABEA1C7EAE86B24E184191C197845B96E1D8B3817F98"'),
    ],
)

FUNCTION_MURMURHASH2_64 = FunctionDocRegistryItem(
    name="murmurhash2_64",
    category=CATEGORY_HASH,
    description=_("Returns the MurmurHash2 64-bit hash of {arg:0} as an integer."),
    examples=[
        SimpleExample('MURMURHASH2_64("DataLens") = 12204402796868507663'),
    ],
)

FUNCTION_SIPHASH64 = FunctionDocRegistryItem(
    name="siphash64",
    category=CATEGORY_HASH,
    description=_("Returns the SipHash64 hash of {arg:0} as an integer."),
    examples=[
        SimpleExample('SIPHASH64("DataLens") = 6283456972272785891'),
    ],
)

FUNCTION_INTHASH64 = FunctionDocRegistryItem(
    name="inthash64",
    category=CATEGORY_HASH,
    description=_("Returns the 64-bit hash of the integer {arg:0}."),
    examples=[
        SimpleExample("INTHASH64(12345) = 16722121143744093920"),
    ],
)

FUNCTION_CITYHASH64 = FunctionDocRegistryItem(
    name="cityhash64",
    category=CATEGORY_HASH,
    description=_("Returns the CityHash64 hash of {arg:0} as an integer."),
    examples=[
        SimpleExample('CITYHASH64("DataLens") = 1276466053635395874'),
    ],
)

FUNCTIONS_HASH = [
    FUNCTION_MD5,
    FUNCTION_SHA1,
    FUNCTION_SHA256,
    FUNCTION_MURMURHASH2_64,
    FUNCTION_SIPHASH64,
    FUNCTION_INTHASH64,
    FUNCTION_CITYHASH64,
]
