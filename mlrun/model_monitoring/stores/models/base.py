# Copyright 2023 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from sqlalchemy import TIMESTAMP, Boolean, Column, Integer, String, Text, Float

from mlrun.common.schemas.model_monitoring import EventFieldType, WriterEvent, FileTargetKind, SchedulingKeys
from mlrun.utils.db import BaseModel


class ModelEndpointsBaseTable(BaseModel):
    __tablename__ = EventFieldType.MODEL_ENDPOINTS

    uid = Column(
        EventFieldType.UID,
        String(40),
        primary_key=True,
    )
    state = Column(EventFieldType.STATE, String(10))
    project = Column(EventFieldType.PROJECT, String(40))
    function_uri = Column(
        EventFieldType.FUNCTION_URI,
        String(255),
    )
    model = Column(EventFieldType.MODEL, String(255))
    model_class = Column(
        EventFieldType.MODEL_CLASS,
        String(255),
    )
    labels = Column(EventFieldType.LABELS, Text)
    model_uri = Column(EventFieldType.MODEL_URI, String(255))
    stream_path = Column(EventFieldType.STREAM_PATH, Text)
    algorithm = Column(
        EventFieldType.ALGORITHM,
        String(255),
    )
    active = Column(EventFieldType.ACTIVE, Boolean)
    monitoring_mode = Column(
        EventFieldType.MONITORING_MODE,
        String(10),
    )
    feature_stats = Column(EventFieldType.FEATURE_STATS, Text)
    current_stats = Column(EventFieldType.CURRENT_STATS, Text)
    feature_names = Column(EventFieldType.FEATURE_NAMES, Text)
    children = Column(EventFieldType.CHILDREN, Text)
    label_names = Column(EventFieldType.LABEL_NAMES, Text)
    endpoint_type = Column(
        EventFieldType.ENDPOINT_TYPE,
        String(10),
    )
    children_uids = Column(EventFieldType.CHILDREN_UIDS, Text)
    drift_measures = Column(EventFieldType.DRIFT_MEASURES, Text)
    drift_status = Column(
        EventFieldType.DRIFT_STATUS,
        String(40),
    )
    monitor_configuration = Column(
        EventFieldType.MONITOR_CONFIGURATION,
        Text,
    )
    monitoring_feature_set_uri = Column(
        EventFieldType.FEATURE_SET_URI,
        String(255),
    )
    error_count = Column(EventFieldType.ERROR_COUNT, Integer)
    metrics = Column(EventFieldType.METRICS, Text)
    first_request = Column(
        EventFieldType.FIRST_REQUEST,
        TIMESTAMP,
    )
    last_request = Column(
        EventFieldType.LAST_REQUEST,
        TIMESTAMP,
    )


class ApplicationResultBaseTable(BaseModel):
    __tablename__ = FileTargetKind.APP_RESULTS
    # APPLICATION_NAME = "application_name"
    # ENDPOINT_ID = "endpoint_id"
    # START_INFER_TIME = "start_infer_time"
    # END_INFER_TIME = "end_infer_time"
    # RESULT_NAME = "result_name"
    # RESULT_VALUE = "result_value"
    # RESULT_KIND = "result_kind"
    # RESULT_STATUS = "result_status"
    # RESULT_EXTRA_DATA = "result_extra_data"
    # CURRENT_STATS = "current_stats"
    application_name = Column(
        WriterEvent.APPLICATION_NAME,
        String(40),
        primary_key=True,
    )

    endpoint_id = Column(
        WriterEvent.ENDPOINT_ID,
        String(40),
    )

    result_name = Column(
        WriterEvent.RESULT_NAME,
        String(40),
        primary_key=True,
    )

    start_infer_time = Column(
        WriterEvent.START_INFER_TIME,
        TIMESTAMP,
    )
    end_infer_time = Column(
        WriterEvent.END_INFER_TIME,
        TIMESTAMP,
    )

    result_status = Column(WriterEvent.RESULT_STATUS, String(10))
    result_kind = Column(WriterEvent.RESULT_KIND, String(40))
    result_value = Column(WriterEvent.RESULT_VALUE, Float)
    result_extra_data = Column(WriterEvent.RESULT_EXTRA_DATA, Text)
    current_stats = Column(WriterEvent.CURRENT_STATS, Text)

class MonitoringSchedulesBaseTable(BaseModel):
    __tablename__ = FileTargetKind.MONITORING_SCHEDULES

    application_name = Column(
        SchedulingKeys.APPLICATION_NAME,
        String(40),
        primary_key=True,
    )

    endpoint_id = Column(
        SchedulingKeys.ENDPOINT_ID,
        String(40),
    )

    last_analyzed = Column(
        SchedulingKeys.LAST_ANALYZED,
        TIMESTAMP,
    )