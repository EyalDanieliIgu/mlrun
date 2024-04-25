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

import datetime
from functools import partial
from unittest.mock import Mock
import os
import pytest
from _pytest.fixtures import FixtureRequest
from mlrun.model_monitoring.writer import (
    ModelMonitoringWriter,
    # V3IOFramesClient,
    WriterEvent,
    _AppResultEvent,
    _Notifier,
    _RawEvent,
    _WriterEventTypeError,
    _WriterEventValueError,
)
import mlrun.model_monitoring.db.tsdb.v3io
import mlrun.common.schemas.model_monitoring as mm_constants
from mlrun.utils.notifications.notification_pusher import CustomNotificationPusher
import mlrun.model_monitoring

TEST_PROJECT = "test-application-results"
V3IO_TABLE_CONTAINER = f"bigdata/{TEST_PROJECT}"


@pytest.fixture(params=[0])
def event(request: FixtureRequest) -> _AppResultEvent:
    now = datetime.datetime.now()
    start_infer_time = now - datetime.timedelta(minutes=5)
    return _AppResultEvent(
        {
            WriterEvent.ENDPOINT_ID: "some-ep-id",
            WriterEvent.START_INFER_TIME: start_infer_time.strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            WriterEvent.END_INFER_TIME: now.strftime("%Y-%m-%d %H:%M:%S"),
            WriterEvent.APPLICATION_NAME: "dummy-app",
            WriterEvent.RESULT_NAME: "data-drift-0",
            WriterEvent.RESULT_KIND: 0,
            WriterEvent.RESULT_VALUE: 0.32,
            WriterEvent.RESULT_STATUS: request.param,
            WriterEvent.RESULT_EXTRA_DATA: "",
            WriterEvent.CURRENT_STATS: "",
        }
    )


@pytest.fixture
def notification_pusher() -> CustomNotificationPusher:
    return Mock(spec=CustomNotificationPusher)


@pytest.mark.parametrize(
    ("event", "exception"),
    [
        ("key1:val1,key2:val2", _WriterEventTypeError),
        ({WriterEvent.ENDPOINT_ID: "ep2211"}, _WriterEventValueError),
    ],
)
def test_reconstruct_event_error(event: _RawEvent, exception: type[Exception]) -> None:
    with pytest.raises(exception):
        ModelMonitoringWriter._reconstruct_event(event)


@pytest.mark.parametrize(
    ("event", "expected_notification_call"),
    [(2, True), (1, False), (0, False)],
    indirect=["event"],
)
def test_notifier(
    event: _AppResultEvent,
    expected_notification_call: bool,
    notification_pusher: Mock,
) -> None:
    _Notifier(event=event, notification_pusher=notification_pusher).notify()
    assert notification_pusher.push.call_count == expected_notification_call


class TestTSDB:
    @staticmethod
    @pytest.fixture
    def tsdb_target() -> mlrun.model_monitoring.db.tsdb.v3io.V3IOTSDBtarget:
        tsdb_target = Mock(spec=mlrun.model_monitoring.db.tsdb.v3io.V3IOTSDBtarget)
        tsdb_target.project = TEST_PROJECT
        tsdb_target.container = V3IO_TABLE_CONTAINER
        tsdb_target.tables = {
            mm_constants.V3IOTSDBTables.APP_RESULTS: mm_constants.V3IOTSDBTables.APP_RESULTS
        }
        tsdb_target.create_tsdb_application_tables()
        return tsdb_target

    @staticmethod
    @pytest.fixture
    def writer(tsdb_target: tsdb_target) -> ModelMonitoringWriter:
        writer = Mock(spec=ModelMonitoringWriter)
        writer._update_tsdb = partial(ModelMonitoringWriter._update_tsdb, writer)
        writer.project = TEST_PROJECT
        writer.tsdb_target = tsdb_target
        return writer

    @staticmethod
    @pytest.mark.skipif(
        os.getenv("V3IO_FRAMESD") is None or os.getenv("V3IO_ACCESS_KEY") is None,
        reason="Configure Framsed to access V3IO store targets",
    )
    def test_tsdb_writer(
        event: _AppResultEvent,
        writer: ModelMonitoringWriter,
        tsdb_target: tsdb_target,
    ) -> None:
        # Generate TSDB table
        # tsdb_store = mlrun.model_monitoring.db.tsdb.v3io(
        #     project=writer.project,
        #     container=writer._v3io_container,
        #     table=FileTargetKind.TSDB_APPLICATION_TABLE,
        #     create_table=True,
        # )

        # tsdb_configurations = {
        #     "access_key": os.getenv("V3IO_ACCESS_KEY"),
        #     # "container": V3IO_TABLE_CONTAINER,
        # }
        #
        # tsdb_target = mlrun.model_monitoring.get_tsdb_target(
        #     project=writer.project, **tsdb_configurations
        # )
        tsdb_target.tables = {
            mm_constants.V3IOTSDBTables.APP_RESULTS: mm_constants.V3IOTSDBTables.APP_RESULTS
        }
        tsdb_target.create_tsdb_application_tables()
        writer._update_tsdb(event)

        # Compare stored TSDB record and provided event
        record_from_tsdb = tsdb_target.get_records(
            table=mm_constants.V3IOTSDBTables.APP_RESULTS,
            filter_query=f"endpoint_id==f{event[WriterEvent.ENDPOINT_ID]}",
            start="now-1d",
            end="now+1d",
        )
        actual_columns = list(record_from_tsdb.columns)

        assert (
            WriterEvent.RESULT_EXTRA_DATA not in actual_columns
        ), "The extra data should not be written to the TSDB"

        expected_columns = WriterEvent.list()
        expected_columns.remove(WriterEvent.RESULT_EXTRA_DATA)
        expected_columns.remove(WriterEvent.END_INFER_TIME)

        assert sorted(expected_columns) == sorted(actual_columns)

        tsdb_target.delete_tsdb_resources()


# class TestTSDB:
#     @staticmethod
#     @pytest.fixture
#     def tsdb_client() -> V3IOFramesClient:
#         return Mock(spec=V3IOFramesClient)
#
#     @staticmethod
#     @pytest.fixture
#     def writer(tsdb_client: V3IOFramesClient) -> ModelMonitoringWriter:
#         writer = Mock(spec=ModelMonitoringWriter)
#         writer._tsdb_client = tsdb_client
#         writer._update_tsdb = partial(ModelMonitoringWriter._update_tsdb, writer)
#         return writer
#
#     @staticmethod
#     def test_no_extra(
#         event: _AppResultEvent,
#         tsdb_client: V3IOFramesClient,
#         writer: ModelMonitoringWriter,
#     ) -> None:
#         writer._update_tsdb(event)
#         tsdb_client.write.assert_called()
#         assert (
#             WriterEvent.RESULT_EXTRA_DATA
#             not in tsdb_client.write.call_args.kwargs["dfs"].columns
#         ), "The extra data should not be written to the TSDB"
