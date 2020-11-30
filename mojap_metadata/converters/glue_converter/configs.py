# Format generictype: (glue_type, is_fully_supported)
# If not fully supported we decide on best option
# if glue_type is Null then we have no way to safely
# convert it

default_type_converter = {
    "int8":  ("TINYINT", True),
    "int16": ("SMALLINT", True),
    "int32": ("INT", True),
    "int64": ("BIGINT", True),
    "uint8": ("SMALLINT", False),
    "uint16": ("INT", False),
    "uint32": ("BIGINT", False),
    "uint64": (None, False),
    "decimal128": ("DECIMAL", True),
    "float16": ("FLOAT", False),
    "float32": ("FLOAT", True),
    "float64": ("DOUBLE", True),
    "time32": (None, False),
    "time32(s)": (None, False),
    "time32(ms)": (None, False),
    "time64(us)": (None, False),
    "time64(ns)": (None, False),
    "date32": ("DATE", True),
    "date64": ("DATE", True),
    "timestamp(s)": ("TIMESTAMP", True),
    "timestamp(ms)": ("TIMESTAMP", True),
    "timestamp(us)": ("TIMESTAMP", True),
    "timestamp(ns)": ("TIMESTAMP", True),
    "string": ("STRING", True),
    "large_string": ("STRING", True),
    "utf8": ("STRING", True),
    "large_utf8": ("STRING", True),
    "binary": ("BINARY", True),
    "binary128": ("BINARY", False),
    "large_binary": ("BINARY", False),
    # Need to do MAPS / STRUCTS
}
