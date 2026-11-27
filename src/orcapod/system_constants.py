# Constants used for source info keys
SYSTEM_COLUMN_PREFIX = "__"
DATAGRAM_PREFIX = "_"
SOURCE_INFO_PREFIX = "source_"
POD_ID_PREFIX = "pod_id_"
PF_VARIATION_PREFIX = "pf_var_"
PF_EXECUTION_PREFIX = "pf_exec_"
DATA_CONTEXT_KEY = "context_key"
INPUT_DATA_HASH_COL = "input_data_hash"
OUTPUT_DATA_HASH_COL = "output_data_hash"
DATA_RECORD_ID = "data_id"
NODE_CONTENT_HASH_COL = "node_content_hash"
SYSTEM_TAG_PREFIX_NAME = "tag"
SYSTEM_TAG_SOURCE_ID_FIELD = "source_id"
SYSTEM_TAG_RECORD_ID_FIELD = "record_id"
POD_VERSION = "pod_version"
EXECUTION_ENGINE = "execution_engine"
POD_TIMESTAMP = "pod_ts"
FIELD_SEPARATOR = ":"
BLOCK_SEPARATOR = "::"
ENV_INFO = "env_info"
IS_EPHEMERAL_COL = "is_ephemeral"
# Table layout versions: the last component of every stored table's path. A layout
# version moves whenever the common bookkeeping columns of that table kind change. All
# started at v1 for the first release, 0.4.0 (developer, November 18, 2026, ITL-1259; 0.3.0
# was withdrawn, so nothing released used v2). The pipeline, tracking and operator layouts
# moved to v2 before that release, with the three nested row ids (developer, November 25,
# 2026, ITL-1471). A function result table has one more
# component before its layout: the revision of its data function type's own record
# (``rev1``, declared by the type, ``core/data_function_types.py``).
PIPELINE_DB_SCHEMA_VERSION = "pdb_v2"
RESULT_DB_SCHEMA_VERSION = "rdb_v1"
TRACKING_DB_SCHEMA_VERSION = "tdb_v2"
OPERATOR_DB_SCHEMA_VERSION = "odb_v2"
SOURCE_DB_SCHEMA_VERSION = "sdb_v1"
STORED_DATA_CONTEXT = "data_context"
# The data context of everything Orcapod wrote before v0.3. Its manifest is not shipped:
# stored rows that carry this key, and tables without a data context column, belong to it.
PRE_V1_DATA_CONTEXT_VERSION = "v0.1"
PRE_V1_DATA_CONTEXT_KEY = "std:v0.1:default"


class SystemConstant:
    def __init__(self, global_prefix: str = ""):
        self._global_prefix = global_prefix

    @property
    def BLOCK_SEPARATOR(self) -> str:
        return BLOCK_SEPARATOR

    @property
    def FIELD_SEPARATOR(self) -> str:
        return FIELD_SEPARATOR

    @property
    def META_PREFIX(self) -> str:
        return f"{self._global_prefix}{SYSTEM_COLUMN_PREFIX}"

    @property
    def DATAGRAM_PREFIX(self) -> str:
        return f"{self._global_prefix}{DATAGRAM_PREFIX}"

    @property
    def SOURCE_PREFIX(self) -> str:
        return f"{self._global_prefix}{DATAGRAM_PREFIX}{SOURCE_INFO_PREFIX}"

    @property
    def CONTEXT_KEY(self) -> str:
        return f"{self._global_prefix}{DATAGRAM_PREFIX}{DATA_CONTEXT_KEY}"

    @property
    def DATA_CONTEXT_COL(self) -> str:
        """Column of every stored table naming the data context that wrote the row."""
        return f"{self._global_prefix}{SYSTEM_COLUMN_PREFIX}{STORED_DATA_CONTEXT}"

    @property
    def POD_ID_PREFIX(self) -> str:
        return f"{self._global_prefix}{SYSTEM_COLUMN_PREFIX}{POD_ID_PREFIX}"

    @property
    def PF_VARIATION_PREFIX(self) -> str:
        return f"{self._global_prefix}{SYSTEM_COLUMN_PREFIX}{PF_VARIATION_PREFIX}"

    @property
    def PF_EXECUTION_PREFIX(self) -> str:
        return f"{self._global_prefix}{SYSTEM_COLUMN_PREFIX}{PF_EXECUTION_PREFIX}"

    @property
    def INPUT_DATA_HASH_COL(self) -> str:
        return f"{self._global_prefix}{SYSTEM_COLUMN_PREFIX}{INPUT_DATA_HASH_COL}"

    @property
    def OUTPUT_DATA_HASH_COL(self) -> str:
        return f"{self._global_prefix}{SYSTEM_COLUMN_PREFIX}{OUTPUT_DATA_HASH_COL}"

    @property
    def DATA_RECORD_ID(self) -> str:
        return f"{self._global_prefix}{SYSTEM_COLUMN_PREFIX}{DATA_RECORD_ID}"

    @property
    def NODE_CONTENT_HASH_COL(self) -> str:
        return f"{self._global_prefix}{SYSTEM_COLUMN_PREFIX}{NODE_CONTENT_HASH_COL}"

    @property
    def SYSTEM_TAG_PREFIX(self) -> str:
        return f"{self._global_prefix}{DATAGRAM_PREFIX}{SYSTEM_TAG_PREFIX_NAME}_"

    @property
    def SYSTEM_TAG_SOURCE_ID_PREFIX(self) -> str:
        return f"{self.SYSTEM_TAG_PREFIX}{SYSTEM_TAG_SOURCE_ID_FIELD}"

    @property
    def SYSTEM_TAG_RECORD_ID_PREFIX(self) -> str:
        return f"{self.SYSTEM_TAG_PREFIX}{SYSTEM_TAG_RECORD_ID_FIELD}"

    @property
    def POD_VERSION(self) -> str:
        return f"{self._global_prefix}{SYSTEM_COLUMN_PREFIX}{POD_VERSION}"

    @property
    def EXECUTION_ENGINE(self) -> str:
        return f"{self._global_prefix}{SYSTEM_COLUMN_PREFIX}{EXECUTION_ENGINE}"

    @property
    def POD_TIMESTAMP(self) -> str:
        return f"{self._global_prefix}{SYSTEM_COLUMN_PREFIX}{POD_TIMESTAMP}"

    @property
    def ENV_INFO(self) -> str:
        return f"{self._global_prefix}{SYSTEM_COLUMN_PREFIX}{ENV_INFO}"

    @property
    def IS_EPHEMERAL_COL(self) -> str:
        return f"{self._global_prefix}{SYSTEM_COLUMN_PREFIX}{IS_EPHEMERAL_COL}"


constants = SystemConstant()
