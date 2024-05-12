



import enum
class ValueType(str, enum.Enum):
    """TDEngine value type. Used to define data types in TDEngine."""

    TIMESTAMP = "TIMESTAMP"
    INT = "INT"
    FLOAT = "FLOAT"
    BINARY = "BINARY"

class TDEngineSchema:
    def __init__(self, table_name: str, columns: dict[str, str], tags: dict[str, str]):
        self.table_name = table_name
        self.columns = columns
        self.tags = tags

    def _create_super_table_query(self, db_prefix: str = "") -> str:
        columns = ", ".join(f"{col} {val}" for col, val in self.columns.items())
        tags = ", ".join(f"{col} {val}" for col, val in self.tags.items())
        return f"CREATE TABLE {db_prefix}{self.table_name} ({columns}) TAGS ({tags});"



class AppResultTable(TDEngineSchema):
    def __init__(self, table_name: str, columns: dict[str, str], tags: dict[str, str]):
        super().__init__(table_name, columns, tags)
        self.columns = {
            "timestamp": ValueType.TIMESTAMP,
            "model_id": ValueType.INT,
            "model_version": ValueType.INT,
            "model_name": ValueType.BINARY,
            "model_endpoint": ValueType.BINARY,
            "model_class": ValueType