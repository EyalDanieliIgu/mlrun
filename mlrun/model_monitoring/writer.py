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
import taosws
import datetime
import json
from typing import Any, NewType
# import influxdb_client
# from influxdb_client.client.write_api import SYNCHRONOUS

# import pandas as pd
# from v3io.dataplane import Client as V3IOClient
# from v3io_frames.client import ClientBase as V3IOFramesClient
# from v3io_frames.errors import Error as V3IOFramesError
# from v3io_frames.frames_pb2 import IGNORE

import mlrun.common.model_monitoring
import mlrun.model_monitoring
import mlrun.model_monitoring.db.stores
import mlrun.utils.v3io_clients
from mlrun.common.schemas.model_monitoring.constants import ResultStatusApp, WriterEvent
from mlrun.common.schemas.notification import NotificationKind, NotificationSeverity
from mlrun.serving.utils import StepToDict
from mlrun.utils import logger
from mlrun.utils.notifications.notification_pusher import CustomNotificationPusher

_TSDB_BE = "tsdb"
_TSDB_RATE = "1/s"
_TSDB_TABLE = "app-results"
_RawEvent = dict[str, Any]
_AppResultEvent = NewType("_AppResultEvent", _RawEvent)


class _WriterEventError:
    pass


class _WriterEventValueError(_WriterEventError, ValueError):
    pass


class _WriterEventTypeError(_WriterEventError, TypeError):
    pass


class _Notifier:
    def __init__(
        self,
        event: _AppResultEvent,
        notification_pusher: CustomNotificationPusher,
        severity: NotificationSeverity = NotificationSeverity.WARNING,
    ) -> None:
        """
        Event notifier - send push notification when appropriate to the notifiers in
        `notification pusher`.
        Note that if you use a Slack App webhook, you need to define it as an MLRun secret
        `SLACK_WEBHOOK`.
        """
        self._event = event
        self._custom_notifier = notification_pusher
        self._severity = severity

    def _should_send_event(self) -> bool:
        return self._event[WriterEvent.RESULT_STATUS] >= ResultStatusApp.detected

    def _generate_message(self) -> str:
        return f"""\
The monitoring app `{self._event[WriterEvent.APPLICATION_NAME]}` \
of kind `{self._event[WriterEvent.RESULT_KIND]}` \
detected a problem in model endpoint ID `{self._event[WriterEvent.ENDPOINT_ID]}` \
at time `{self._event[WriterEvent.START_INFER_TIME]}`.

Result data:
Name: `{self._event[WriterEvent.RESULT_NAME]}`
Value: `{self._event[WriterEvent.RESULT_VALUE]}`
Status: `{self._event[WriterEvent.RESULT_STATUS]}`
Extra data: `{self._event[WriterEvent.RESULT_EXTRA_DATA]}`\
"""

    def notify(self) -> None:
        """Send notification if appropriate"""
        if not self._should_send_event():
            logger.debug("Not sending a notification")
            return
        message = self._generate_message()
        self._custom_notifier.push(message=message, severity=self._severity)
        logger.debug("A notification should have been sent")


class ModelMonitoringWriter(StepToDict):
    """
    Write monitoring app events to V3IO KV storage
    """

    kind = "monitoring_application_stream_pusher"

    def __init__(self, project: str) -> None:
        self.project = project
        self.name = project  # required for the deployment process
        # self._v3io_container = self.get_v3io_container(self.name)
        # self._tsdb_client = self._get_v3io_frames_client(self._v3io_container)
        self._custom_notifier = CustomNotificationPusher(
            notification_types=[NotificationKind.slack]
        )
        # self._create_tsdb_table()

    # @staticmethod
    # def get_v3io_container(project_name: str) -> str:
    #     return f"users/pipelines/{project_name}/monitoring-apps"
    #
    # @staticmethod
    # def _get_v3io_client() -> V3IOClient:
    #     return mlrun.utils.v3io_clients.get_v3io_client(
    #         endpoint=mlrun.mlconf.v3io_api,
    #     )
    #
    # @staticmethod
    # def _get_v3io_frames_client(v3io_container: str) -> V3IOFramesClient:
    #     return mlrun.utils.v3io_clients.get_frames_client(
    #         address=mlrun.mlconf.v3io_framesd,
    #         container=v3io_container,
    #     )

    # def _create_tsdb_table(self) -> None:
    #     self._tsdb_client.create(
    #         backend=_TSDB_BE,
    #         table=_TSDB_TABLE,
    #         if_exists=IGNORE,
    #         rate=_TSDB_RATE,
    #     )

    def _update_kv_db(self, event: _AppResultEvent) -> None:
        event = _AppResultEvent(event.copy())
        application_result_store = mlrun.model_monitoring.get_store_object(
            project=self.project
        )
        application_result_store.write_application_result(event=event)

    def _update_tsdb(self, event: _AppResultEvent) -> None:
        print("[EYAL]: going to write to tdengine: ", event)

        dsn = "taosws://root:taosdata@192.168.224.154:30377"
        db = "mlrun_model_monitoring"
        table_name = f"{self.project}_{event[WriterEvent.ENDPOINT_ID]}_{event[WriterEvent.APPLICATION_NAME]}_{event[WriterEvent.RESULT_NAME]}"
        conn = taosws.connect(f"{dsn}/{db}")
        tags = f"'{self.project}',{event[WriterEvent.ENDPOINT_ID]},'{event[WriterEvent.APPLICATION_NAME]}','{event[WriterEvent.RESULT_NAME]}'"
        print(f"[EYAL]: going to create tdengine table {table_name} with tags {tags}")
        conn.execute(
            f"create table if not exists {table_name} using app_results tags({tags})"
        )
        print(f"[EYAL]: going to insert tdengine record")

        result_value = event["result_value"]
        result_status = event["result_status"]
        end_infer_time = event["end_infer_time"]
        start_infer_time = event["start_infer_time"]
        result_extra_data = event["result_extra_data"]
        current_stats = event["current_stats"]

        conn.execute(
            f"insert into {table_name} values ('{end_infer_time[:-9]}', '{start_infer_time[:-9]}', {result_value}, {result_status}, {json.dumps(current_stats)}, {json.dumps(result_extra_data)})"
        )

        print(f"[EYAL]: done to insert tdengine record")

        # bucket = "test_bucket"
        # org = "test_org"
        # token = "zJzQMa2gxD9ru_Pwt3pCWiHRNoNMxmDDtSVGznzQs3dI_I09CBz3a9iKDETFE9iqz_R13q1lsR0TCE8MVMHWpw=="
        # # Store the URL of your InfluxDB instance
        # url = "http://192.168.224.154:8086/"
        # client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
        # write_api = client.write_api(write_options=SYNCHRONOUS)
        # p = (
        #     influxdb_client.Point("application_result_v2")
        #     .tag("endpoint_id", event[WriterEvent.ENDPOINT_ID])
        #     .tag("application_name", event[WriterEvent.APPLICATION_NAME])
        #     .tag("result_name", event[WriterEvent.RESULT_NAME])
        #     .tag("result_kind", event[WriterEvent.RESULT_KIND])
        #     .field("start_infer_time", event[WriterEvent.START_INFER_TIME])
        #     .field("result_status", event[WriterEvent.RESULT_STATUS])
        #     .field("result_value", event[WriterEvent.RESULT_VALUE])
        #     .field("result_extra_data", event[WriterEvent.RESULT_EXTRA_DATA])
        #     .field("current_stats", event[WriterEvent.CURRENT_STATS])
        #     .time(event[WriterEvent.END_INFER_TIME])
        # )
        # write_api.write(bucket=bucket, org=org, record=p)

    #     event = _AppResultEvent(event.copy())
    #     event[WriterEvent.END_INFER_TIME] = datetime.datetime.fromisoformat(
    #         event[WriterEvent.END_INFER_TIME]
    #     )
    #     del event[WriterEvent.RESULT_EXTRA_DATA]
    #     try:
    #         self._tsdb_client.write(
    #             backend=_TSDB_BE,
    #             table=_TSDB_TABLE,
    #             dfs=pd.DataFrame.from_records([event]),
    #             index_cols=[
    #                 WriterEvent.END_INFER_TIME,
    #                 WriterEvent.ENDPOINT_ID,
    #                 WriterEvent.APPLICATION_NAME,
    #                 WriterEvent.RESULT_NAME,
    #             ],
    #         )
    #         logger.info("Updated V3IO TSDB successfully", table=_TSDB_TABLE)
    #     except V3IOFramesError as err:
    #         logger.warn(
    #             "Could not write drift measures to TSDB",
    #             err=err,
    #             table=_TSDB_TABLE,
    #             event=event,
    #         )

    @staticmethod
    def _reconstruct_event(event: _RawEvent) -> _AppResultEvent:
        """
        Modify the raw event into the expected monitoring application event
        schema as defined in `mlrun.common.schemas.model_monitoring.constants.WriterEvent`
        """
        try:
            result_event = _AppResultEvent(
                {key: event[key] for key in WriterEvent.list()}
            )
            result_event[WriterEvent.CURRENT_STATS] = json.loads(
                event[WriterEvent.CURRENT_STATS]
            )
            return result_event
        except KeyError as err:
            raise _WriterEventValueError(
                "The received event misses some keys compared to the expected "
                "monitoring application event schema"
            ) from err
        except TypeError as err:
            raise _WriterEventTypeError(
                f"The event is of type: {type(event)}, expected a dictionary"
            ) from err

    def do(self, event: _RawEvent) -> None:
        event = self._reconstruct_event(event)
        logger.info("Starting to write event", event=event)
        self._update_tsdb(event)
        self._update_kv_db(event)
        _Notifier(event=event, notification_pusher=self._custom_notifier).notify()
        logger.info("Completed event DB writes")
