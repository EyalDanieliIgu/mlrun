import mlrun.common.schemas.model_monitoring as mm_constants
import mlrun.common.types

from dataclasses import dataclass
_MODEL_MONITORING_DATABASE = "mlrun_model_monitoring"
import datetime
class TDEngineColumnType:
    def __init__(self, data_type: str, length: int = None):
        self.data_type = data_type
        self.length = length

    def __str__(self):
        if self.length is not None:
            return f"{self.data_type}({self.length})"
        else:
            return self.data_type


class TDEngineColumn(mlrun.common.types.StrEnum):
    TIMESTAMP = TDEngineColumnType("TIMESTAMP")
    FLOAT = TDEngineColumnType("FLOAT")
    INT = TDEngineColumnType("INT")
    BINARY_40 = TDEngineColumnType("BINARY", 40)
    BINARY_64 = TDEngineColumnType("BINARY", 64)
    BINARY_10000 = TDEngineColumnType("BINARY", 10000)


class TDEngineSchema:
    """
    A class to represent a schema in TDengine. Using this schema, you can generate the relevant queries to create,
    insert, and query data from TDengine. At the moment, there are 3 schemas: AppResultTable, Metrics, and Predictions.
    """
    def __init__(self, super_table: str, columns: dict[str, str], tags: dict[str, str], ):
        self.super_table = super_table
        self.columns = columns
        self.tags = tags

    def _create_super_table_query(self, database: str = _MODEL_MONITORING_DATABASE) -> str:
        columns = ", ".join(f"{col} {val}" for col, val in self.columns.items())
        tags = ", ".join(f"{col} {val}" for col, val in self.tags.items())
        return f"CREATE STABLE if not exists {database}.{self.super_table} ({columns}) TAGS ({tags});"

    def _create_subtable_query(self, subtable: str, values: dict[str, str], database: str = _MODEL_MONITORING_DATABASE) -> str:
        values = ", ".join(f"'{values[val]}'" for val in self.tags)
        return f"CREATE TABLE if not exists {database}.{subtable} using {self.super_table} TAGS ({values});"

    def _insert_subtable_query(self, subtable: str, values: dict[str, str], database: str = _MODEL_MONITORING_DATABASE) -> str:
        values = ", ".join(f"'{values[val]}'" for val in self.columns)
        return f"INSERT INTO {database}.{subtable} VALUES ({values});"

    def _delete_subtable_query(self, subtable: str, values: dict[str, str], database: str = _MODEL_MONITORING_DATABASE) -> str:
        return f"DELETE FROM {database}.{subtable};"


    def _get_records_query(self, subtable: str, database: str = _MODEL_MONITORING_DATABASE, columns_to_filter: list[str] = None,
                           filter_query: str = "",
                           start: str = datetime.datetime.now().astimezone() - datetime.timedelta(hours=1),
                           end: str = datetime.datetime.now().astimezone(),timestamp_column: str = "time",
                           ) -> str:

        full_query = "select "
        if columns_to_filter:
            full_query += ", ".join(columns_to_filter)
        else:
            full_query += "*"
        full_query += f" from {database}.{subtable}"

        if any([filter_query, start, end]):
            full_query += " where "
            if filter_query:
                full_query += filter_query + " and "
            if start:
                full_query += f" {timestamp_column} >= '{start}'" + " and "
            if end:
                full_query += f" {timestamp_column} <= '{end}'"
            if full_query.endswith(" and "):
                full_query = full_query[:-5]
        return full_query + ";"



@dataclass
class AppResultTable(TDEngineSchema):
    super_table: str = mm_constants.TDEngineSuperTables.APP_RESULTS
    columns = {
        mm_constants.WriterEvent.END_INFER_TIME: TDEngineColumn.TIMESTAMP,
        mm_constants.WriterEvent.START_INFER_TIME: TDEngineColumn.TIMESTAMP,
        mm_constants.ResultData.RESULT_VALUE: TDEngineColumn.FLOAT,
        mm_constants.ResultData.RESULT_STATUS: TDEngineColumn.INT,
        mm_constants.ResultData.RESULT_KIND: TDEngineColumn.BINARY_40,
        mm_constants.ResultData.CURRENT_STATS: TDEngineColumn.BINARY_10000,
    }

    tags = {
        mm_constants.EventFieldType.PROJECT: TDEngineColumn.BINARY_64,
        mm_constants.WriterEvent.ENDPOINT_ID: TDEngineColumn.BINARY_64,
        mm_constants.WriterEvent.APPLICATION_NAME: TDEngineColumn.BINARY_64,
        mm_constants.ResultData.RESULT_NAME: TDEngineColumn.BINARY_64,
    }


@dataclass
class Metrics(TDEngineSchema):
    super_table: str = mm_constants.TDEngineSuperTables.METRICS
    columns = {
        mm_constants.WriterEvent.END_INFER_TIME: TDEngineColumn.TIMESTAMP,
        mm_constants.WriterEvent.START_INFER_TIME: TDEngineColumn.TIMESTAMP,
        mm_constants.MetricData.METRIC_VALUE: TDEngineColumn.FLOAT,
    }

    tags = {
        mm_constants.EventFieldType.PROJECT: TDEngineColumn.BINARY_64,
        mm_constants.WriterEvent.ENDPOINT_ID: TDEngineColumn.BINARY_64,
        mm_constants.WriterEvent.APPLICATION_NAME: TDEngineColumn.BINARY_64,
        mm_constants.MetricData.METRIC_NAME: TDEngineColumn.BINARY_64,
    }


@dataclass
class Predictions(TDEngineSchema):
    super_table: str = mm_constants.TDEngineSuperTables.PREDICTIONS
    columns = {
        mm_constants.EventFieldType.TIME: TDEngineColumn.TIMESTAMP,
        mm_constants.EventFieldType.LATENCY: TDEngineColumn.FLOAT,
        mm_constants.EventKeyMetrics.CUSTOM_METRICS: TDEngineColumn.BINARY_10000,
    }
    tags = {
        mm_constants.EventFieldType.PROJECT: TDEngineColumn.BINARY_64,
        mm_constants.WriterEvent.ENDPOINT_ID: TDEngineColumn.BINARY_64,
    }

