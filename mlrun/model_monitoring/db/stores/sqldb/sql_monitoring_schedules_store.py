# # Copyright 2023 Iguazio
# #
# # Licensed under the Apache License, Version 2.0 (the "License");
# # you may not use this file except in compliance with the License.
# # You may obtain a copy of the License at
# #
# #   http://www.apache.org/licenses/LICENSE-2.0
# #
# # Unless required by applicable law or agreed to in writing, software
# # distributed under the License is distributed on an "AS IS" BASIS,
# # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# # See the License for the specific language governing permissions and
# # limitations under the License.
# #
#
# import json
# import typing
#
# import mlrun.common.model_monitoring.helpers
# import mlrun.common.schemas.model_monitoring
# import mlrun.model_monitoring.helpers
# from mlrun.common.db.sql_session import create_session, get_engine
#
# from mlrun.model_monitoring.db.stores.base.monitoring_schedules_store import MonitoringSchedules
# from mlrun.model_monitoring.db.stores.sqldb.models import get_monitoring_schedules_table
#
#
# class SQLMonitoringSchedules(MonitoringSchedules):
#     """
#     Handles the DB operations when the DB target is from type SQL. For the SQL operations, we use SQLAlchemy, a Python
#     SQL toolkit that handles the communication with the database.  When using SQL for storing the model endpoints
#     record, the user needs to provide a valid connection string for the database.
#     """
#
#     _engine = None
#
#     def __init__(
#         self,
#         project: str,
#         sql_connection_string: str = None,
#         secret_provider: typing.Callable = None,
#     ):
#         """
#         Initialize SQL store target object.
#
#         :param project:               The name of the project.
#         :param sql_connection_string: Valid connection string or a path to SQL database with model endpoints table.
#         :param secret_provider:       An optional secret provider to get the connection string secret.
#         """
#
#         super().__init__(project=project)
#
#         self.sql_connection_string = (
#             sql_connection_string
#             or mlrun.model_monitoring.helpers.get_connection_string(
#                 secret_provider=secret_provider
#             )
#         )
#
#         self.table_name = (
#             mlrun.common.schemas.model_monitoring.FileTargetKind.MONITORING_SCHEDULES
#         )
#
#         self._engine = get_engine(dsn=self.sql_connection_string)
#         self.MonitoringSchedulesTable = get_monitoring_schedules_table(
#             connection_string=self.sql_connection_string
#         )
#         # Create table if not exist. The `metadata` contains the `ModelEndpointsTable`
#         if not self._engine.has_table(self.table_name):
#             self.MonitoringSchedulesTable.metadata.create_all(  # pyright: ignore[reportGeneralTypeIssues]
#                 bind=self._engine
#             )
#         self.monitoring_schedules_table = (
#             self.MonitoringSchedulesTable.__table__  # pyright: ignore[reportGeneralTypeIssues]
#         )
#
#     def get_last_analyzed(self, endpoint_id: str, application_name: str):
#         # Get the model endpoint record using sqlalchemy ORM
#         with create_session(dsn=self.sql_connection_string) as session:
#             print("[EYAL]: going to get applciation_record: ", application_name)
#             # Generate the get query
#             application_record = (
#                 session.query(self.MonitoringSchedulesTable)
#                 .filter_by(application_name=application_name, endpoint_id=endpoint_id)
#                 .one_or_none()
#             )
#
#         if not application_record:
#             raise mlrun.errors.MLRunNotFoundError(
#                 f"Application {application_name} not found"
#             )
#         print("[EYAL]: done to get applicatino_record: ", application_record)
#         # Convert the database values and the table columns into a python dictionary
#         application_dict = application_record.to_dict()
#         return application_dict["last_analyzed"]
#
#     def update_last_analyzed(self, endpoint_id, application_name, attributes):
#         print("[EYAL]: going to update last analyzed")
#         # Update the model endpoint record using sqlalchemy ORM
#         with create_session(dsn=self.sql_connection_string) as session:
#             # Generate and commit the update session query
#             session.query(self.MonitoringSchedulesTable).filter_by(
#                 application_name=application_name, endpoint_id=endpoint_id
#             ).update(attributes)
#             session.commit()
#
#         print("[EYAL]: done to update last analyzed")
#
#
