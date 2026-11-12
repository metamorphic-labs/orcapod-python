"""DataclassLogicalType and DataclassLogicalTypeFactory.

Dataclasses are user classes: their logical type name and Arrow extension name are the
class's identity string (name, kind ``"dataclass"`` and field types). The shared write
and read paths live in ``user_class_logical_type``.

Category tag: ``"orcapod.dataclass"``
"""

from __future__ import annotations


import orcapod.logical_types.user_class_identity as uci
from orcapod.logical_types.user_class_logical_type import (
    UserClassLogicalType,
    UserClassLogicalTypeFactory,
)

#: Category tag embedded in Arrow extension metadata. Used as the factory dispatch key.
DATACLASS_CATEGORY = "orcapod.dataclass"


class DataclassLogicalType(UserClassLogicalType):
    """Logical type binding a dataclass to its Arrow extension type.

    Example:
        >>> lt = DataclassLogicalType(
        ...     "Point@76d961e4...", Point,
        ...     pa.struct([pa.field("x", pa.int64()), pa.field("y", pa.int64())]),
        ...     [("x", int), ("y", int)],
        ... )
        >>> lt.python_to_storage(Point(1, 2), converter)
        {"x": 1, "y": 2}
    """

    category = DATACLASS_CATEGORY


class DataclassLogicalTypeFactory(UserClassLogicalTypeFactory):
    """Factory for dataclass logical types.

    Stored fields are the dataclass fields with ``init=True``.

    Register with::

        converter.register_logical_type_factory(
            DataclassLogicalTypeFactory(),
            category="orcapod.dataclass",
            python_bases=[object],
        )
    """

    kind = uci.KIND_DATACLASS
    category = DATACLASS_CATEGORY
    logical_type_class = DataclassLogicalType
