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
# import pandas as pd
#
# import mlrun.common.model_monitoring.helpers
# import mlrun.common.schemas.model_monitoring
# import mlrun.model_monitoring.helpers
# from mlrun.common.db.sql_session import get_engine
#
# from mlrun.model_monitoring.db.stores.base.application_result_store import ApplicationResult
# from mlrun.model_monitoring.db.stores.sqldb.models import get_application_result_table
#
#
# class SQLApplicationResult(ApplicationResult):
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
#             mlrun.common.schemas.model_monitoring.FileTargetKind.APP_RESULTS
#         )
#
#         self._engine = get_engine(dsn=self.sql_connection_string)
#         self.ApplicationResultsTable = get_application_result_table(
#             connection_string=self.sql_connection_string
#         )
#         # Create table if not exist. The `metadata` contains the `ModelEndpointsTable`
#         if not self._engine.has_table(self.table_name):
#             self.ApplicationResultsTable.metadata.create_all(  # pyright: ignore[reportGeneralTypeIssues]
#                 bind=self._engine
#             )
#         self.application_result_table = (
#             self.ApplicationResultsTable.__table__  # pyright: ignore[reportGeneralTypeIssues]
#         )
#
#     def write_application_result(self, event: dict[str, typing.Any]):
#         """
#         Create a new endpoint record in the SQL table. This method also creates the model endpoints table within the
#         SQL database if not exist.
#
#         :param endpoint: model endpoint dictionary that will be written into the DB.
#         """
#         print("[EYAL]: going to write new event to application result table: ", event)
#
#         with self._engine.connect() as connection:
#             # Adjust timestamps fields
#             # endpoint[
#             #     mlrun.common.schemas.model_monitoring.EventFieldType.FIRST_REQUEST
#             # ] = datetime.now(timezone.utc)
#             # endpoint[
#             #     mlrun.common.schemas.model_monitoring.EventFieldType.LAST_REQUEST
#             # ] = datetime.now(timezone.utc)
#
#             # Convert the result into a pandas Dataframe and write it into the database
#             event_df = pd.DataFrame([event])
#
#             print('[EYAL]: the event which is going to be written as df: ', event_df)
#
#             event_df.to_sql(
#                 self.table_name, con=connection, index=False, if_exists="append"
#             )
#         print("[EYAL]: Done to write new event to application result table: ", event)
#
#
