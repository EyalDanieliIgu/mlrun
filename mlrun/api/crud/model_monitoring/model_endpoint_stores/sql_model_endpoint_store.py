# Copyright 2018 Iguazio
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
#

import typing


import pandas as pd
import sqlalchemy

import mlrun
import mlrun.api.schemas
import mlrun.model_monitoring.constants as model_monitoring_constants
import mlrun.utils.model_monitoring
import mlrun.utils.v3io_clients
from mlrun.utils import logger
from .model_endpoint_store import _ModelEndpointStore
import sqlalchemy as db
from sqlalchemy.orm import sessionmaker

class _ModelEndpointSQLStore(_ModelEndpointStore):

    """
    Handles the DB operations when the DB target is from type SQL. For the SQL operations, we use SQLAlchemy, a Python
    SQL toolkit that handles the communication with the database.  When using SQL for storing the model endpoints
    record, the user needs to provide a valid connection string for the database.
    """

    def __init__(
            self,
            project: str,
            connection_string: str = None,
    ):
        """
        Initialize SQL store target object.

        :param project: The name of the project.
        :param connection_string: Valid connection string or a path to SQL database with model endpoints table.
        """

        super().__init__(project=project)
        self.connection_string = connection_string
        self.db = db
        self.sessionmaker = sessionmaker
        self.table_name = model_monitoring_constants.EventFieldType.MODEL_ENDPOINTS

    def write_model_endpoint(self, endpoint: mlrun.api.schemas.ModelEndpoint):
        """
        Create a new endpoint record in the SQL table. This method also creates the model endpoints table within the
        SQL database if not exist.

        :param endpoint: ModelEndpoint object that will be written into the DB.
        """

        engine = self.db.create_engine(
            self.connection_string
        )

        with engine.connect():
            if not engine.has_table(self.table_name):
                logger.info("Creating new model endpoints table in DB")
                # Define schema and table for the model endpoints table as required by the SQL table structure
                metadata = self.db.MetaData()
                self._get_table(self.table_name, metadata)

                # Create the table that stored in the MetaData object (if not exist)
                metadata.create_all(engine)

            # Retrieving the relevant attributes from the model endpoint object
            endpoint_dict = self.get_params(endpoint=endpoint)
            endpoint_dict['predictions_per_second'] = None
            endpoint_dict['latency_avg_1h'] = None

            # Convert the result into a pandas Dataframe and write it into the database
            endpoint_df = pd.DataFrame([endpoint_dict])
            endpoint_df.to_sql(
                self.table_name, con=engine, index=False, if_exists="append"
            )

        print("[EYAL]: SQL endpoint created!")

    def update_model_endpoint(self, endpoint_id: str, attributes: typing.Dict):
        """
        Update a model endpoint record with a given attributes.

        :param endpoint_id: The unique id of the model endpoint.
        :param attributes: Dictionary of attributes that will be used for update the model endpoint. Note that the keys
                           of the attributes dictionary should exist in the SQL table.

        """
        print("[EYAL]: going to update SQL db TARGET: ", attributes)

        engine = self.db.create_engine(self.connection_string)
        with engine.connect():
            # Generate the sqlalchemy.schema.Table object that represents the model endpoints table
            metadata = self.db.MetaData()
            model_endpoints_table = self.db.Table(
                self.table_name, metadata, autoload=True, autoload_with=engine
            )

            # Define and execute the query with the given attributes and the related model endpoint id
            update_query = (
                self.db.update(model_endpoints_table)
                    .values(attributes)
                    .where(
                    model_endpoints_table.c[model_monitoring_constants.EventFieldType.ENDPOINT_ID] == endpoint_id)
            )
            engine.execute(update_query)

        print("[EYAL]: model endpoint has been updated!")

    def delete_model_endpoint(self, endpoint_id: str):
        """
        Deletes the SQL record of a given model endpoint id.

        :param endpoint_id: The unique id of the model endpoint.
        """
        engine = self.db.create_engine(self.connection_string)
        with engine.connect():
            # Generate the sqlalchemy.schema.Table object that represents the model endpoints table
            metadata = self.db.MetaData()
            model_endpoints_table = self.db.Table(
                self.table_name, metadata, autoload=True, autoload_with=engine
            )

            print("[EYAL]: going to delete model endpoint!")
            # Delete the model endpoint record using sqlalchemy ORM
            session = self.sessionmaker(bind=engine)()
            session.query(model_endpoints_table).filter_by(
                endpoint_id=endpoint_id
            ).delete()
            session.commit()
            session.close()

            print("[EYAL]: model endpoint has been deleted!")

    def get_model_endpoint(
            self,
            metrics: typing.List[str] = None,
            start: str = "now-1h",
            end: str = "now",
            feature_analysis: bool = False,
            endpoint_id: str = None,
            convert_to_endpoint_object: bool = True,
    ) -> typing.Union[mlrun.api.schemas.ModelEndpoint, dict]:
        """
        Get a single model endpoint object. You can apply different time series metrics that will be added to the
        result.

        :param endpoint_id:                The unique id of the model endpoint.
        :param start:                      The start time of the metrics. Can be represented by a string containing an
                                           RFC 3339 time, a Unix timestamp in milliseconds, a relative time (`'now'` or
                                           `'now-[0-9]+[mhd]'`, where `m` = minutes, `h` = hours, and `'d'` = days), or
                                           0 for the earliest time.
        :param end:                        The end time of the metrics. Can be represented by a string containing an
                                           RFC 3339 time, a Unix timestamp in milliseconds, a relative time (`'now'` or
                                           `'now-[0-9]+[mhd]'`, where `m` = minutes, `h` = hours, and `'d'` = days),
                                           or 0 for the earliest time.
        :param metrics:                    A list of metrics to return for the model endpoint. There are pre-defined
                                           metrics for model endpoints such as predictions_per_second and
                                           latency_avg_5m but also custom metrics defined by the user. Please note that
                                           these metrics are stored in the time series DB and the results will be
                                           appeared under model_endpoint.spec.metrics.
        :param feature_analysis:           When True, the base feature statistics and current feature statistics will
                                           be added to the output of the resulting object.
        :param convert_to_endpoint_object: A boolean that indicates whether to convert the model endpoint dictionary
                                           into a ModelEndpoint or not. True by default.

        :return: A ModelEndpoint object or a model endpoint dictionary if convert_to_endpoint_object is False.
        """
        logger.info(
            "Getting model endpoint record from SQL",
            endpoint_id=endpoint_id,
        )

        engine = self.db.create_engine(self.connection_string)

        # Validate that the model endpoints table exists
        if not engine.has_table(self.table_name):
            raise mlrun.errors.MLRunNotFoundError(f"Table {self.table_name} not found")

        with engine.connect():

            # Generate the sqlalchemy.schema.Table object that represents the model endpoints table
            metadata = self.db.MetaData()
            model_endpoints_table = self.db.Table(
                self.table_name, metadata, autoload=True, autoload_with=engine
            )

            # Get the model endpoint record using sqlalchemy ORM
            from sqlalchemy.orm import sessionmaker

            session = sessionmaker(bind=engine)()

            columns = model_endpoints_table.columns.keys()
            values = (
                session.query(model_endpoints_table)
                    .filter_by(endpoint_id=endpoint_id).filter_by()
                    .all()
            )
            session.close()

        if len(values) == 0:
            raise mlrun.errors.MLRunNotFoundError(f"Endpoint {endpoint_id} not found")

        # Convert the database values and the table columns into a python dictionary
        endpoint = dict(zip(columns, values[0]))

        if convert_to_endpoint_object:
            # Convert the model endpoint dictionary into a ModelEndpont object
            endpoint = self._convert_into_model_endpoint_object(
                endpoint=endpoint, feature_analysis=feature_analysis
            )

        # If time metrics were provided, retrieve the results from the time series DB
        if metrics:
            print('[EYAL]: now in metrics: ', metrics)
            endpoint_metrics = self.get_endpoint_metrics(
                endpoint_id=endpoint_id,
                start=start,
                end=end,
                metrics=metrics,
            )
            if endpoint_metrics:
                endpoint.status.metrics = endpoint_metrics

        return endpoint

    def list_model_endpoints(
            self, model: str = None, function: str = None, labels: typing.List = None, top_level: bool = None,
            metrics: typing.List[str] = None,
            start: str = "now-1h",
            end: str = "now",
    ) -> mlrun.api.schemas.ModelEndpointList:
        """
        Returns a list of endpoint unique ids, supports filtering by model, function, labels or top level.
        By default, when no filters are applied, all available endpoint ids for the given project will be listed.

        :param model:           The name of the model to filter by.
        :param function:        The name of the function to filter by.
        :param labels:          A list of labels to filter by. Label filters work by either filtering a specific value
                                of a label (i.e. list("key==value")) or by looking for the existence of a given
                                key (i.e. "key").
        :param top_level:       If True will return only routers and endpoint that are NOT children of any router.
        :param metrics:         A list of metrics to return for each model endpoint. There are pre-defined metrics
                                for model endpoints such as predictions_per_second and latency_avg_5m but also custom
                                metrics defined by the user. Please note that these metrics are stored in the time
                                series DB and the results will be appeared under model_endpoint.spec.metrics.
        :param start:           The start time of the metrics. Can be represented by a string containing an RFC 3339
                                time, a Unix timestamp in milliseconds, a relative time (`'now'` or
                                `'now-[0-9]+[mhd]'`, where `m` = minutes, `h` = hours, and `'d'` = days), or 0 for the
                                 earliest time.
        :param end:              The end time of the metrics. Can be represented by a string containing an RFC 3339
                                 time, a Unix timestamp in milliseconds, a relative time (`'now'` or
                                 `'now-[0-9]+[mhd]'`, where `m` = minutes, `h` = hours, and `'d'` = days), or 0 for
                                 the earliest time.

        :return: An object of ModelEndpointList which is literally a list of model endpoints along with some
                          metadata. To get a standard list of model endpoints use ModelEndpointList.endpoints.
        """

        engine = self.db.create_engine(self.connection_string)

        # Generate an empty ModelEndpointList that will be filled afterwards with ModelEndpoint objects
        endpoint_list = mlrun.api.schemas.model_endpoints.ModelEndpointList(
            endpoints=[]
        )
        with engine.connect():

            # Generate the sqlalchemy.schema.Table object that represents the model endpoints table
            metadata = self.db.MetaData()
            model_endpoints_table = self.db.Table(
                self.table_name, metadata, autoload=True, autoload_with=engine
            )

            # Get the model endpoint records using sqlalchemy ORM
            from sqlalchemy.orm import sessionmaker
            session = sessionmaker(bind=engine)()

            columns = model_endpoints_table.columns.keys()
            values = session.query(model_endpoints_table).filter_by(project=self.project)
            print("[EYAL]: values before filtering: ", values)
            # Apply filters
            if model:
                values = self._filter_values(
                    values, model_endpoints_table, "model", [model]
                )
            if function:
                values = self._filter_values(
                    values, model_endpoints_table, "function", [function]
                )
            if top_level:
                node_ep = str(mlrun.utils.model_monitoring.EndpointType.NODE_EP.value)
                router_ep = str(mlrun.utils.model_monitoring.EndpointType.ROUTER.value)
                endpoint_types = [node_ep, router_ep]
                values = self._filter_values(
                    values,
                    model_endpoints_table,
                    "endpoint_type",
                    [endpoint_types],
                    combined=False,
                )
            if labels:
                pass

            print("[EYAL]: columns: ", columns)
            print("[EYAL]: values after filtering: ", values)

            # Convert the results from the DB into a ModelEndpoint object and append it to the ModelEndpointList
            for endpoint_values in values.all():
                endpoint_dict = dict(zip(columns, endpoint_values))
                endpoint_obj = self._convert_into_model_endpoint_object(endpoint_dict)

                # If time metrics were provided, retrieve the results from the time series DB
                if metrics:
                    print('[EYAL]: now in metrics! ', metrics)
                    endpoint_metrics = self.get_endpoint_metrics(
                        endpoint_id=endpoint_obj.metadata.uid,
                        start=start,
                        end=end,
                        metrics=metrics,
                    )
                    if endpoint_metrics:
                        endpoint_obj.status.metrics = endpoint_metrics

                endpoint_list.endpoints.append(endpoint_obj)

        return endpoint_list

    @staticmethod
    def _filter_values(
            values, model_endpoints_table: sqlalchemy.Table, key_filter: str, filtered_values: typing.List, combined=True
    ):
        """

        :param values:
        :param model_endpoints_table: SQLAlchemy table object that represents the model endpoints table.
        :param key_filter:            Key column to filter by.
        :param filtered_values:       List of values to filter the query the result.
        :param combined:

        return:
        """
        print('[EYAL]: now in filter values')
        print('[EYAL]: values: ', values)
        print('[EYAL]: values type: ', type(values))
        print('[EYAL]: values: ', model_endpoints_table)
        print('[EYAL]: values: ', key_filter)
        print('[EYAL]: values: ', filtered_values)
        print('[EYAL]: values: ', combined)
        if len(filtered_values) == 1:
            return values.filter(model_endpoints_table.c[key_filter] == filtered_values)
        if combined:
            print('[EYAL]: in combined')
            pass
        else:
            # Create a filter query and take into account at least one of the filtered values
            filter_query = ()
            for filter in filtered_values:
                filter_query += model_endpoints_table.c[key_filter] == filter
            return values.filter(filter_query).all()

    def _get_table(self, table_name: str, metadata: sqlalchemy.MetaData):
        """Declaring a new SQL table object with the required model endpoints columns

        :param table_name: Model endpoints SQL table name.
        :param metadata:   SQLAlchemy MetaData object that used to describe the SQL DataBase. The below method uses the
                           MetaData object for declaring a table.
        """

        self.db.Table(
            table_name,
            metadata,
            self.db.Column("endpoint_id", self.db.String(40), primary_key=True),
            self.db.Column("state", self.db.String(10)),
            self.db.Column("project", self.db.String(40)),
            self.db.Column("function_uri", self.db.String(255)),
            self.db.Column("model", self.db.String(255)),
            self.db.Column("model_class", self.db.String(255)),
            self.db.Column("labels", self.db.Text),
            self.db.Column("model_uri", self.db.String(255)),
            self.db.Column("stream_path", self.db.Text),
            self.db.Column("active", self.db.Boolean),
            self.db.Column("monitoring_mode", self.db.String(10)),
            self.db.Column("feature_stats", self.db.Text),
            self.db.Column("current_stats", self.db.Text),
            self.db.Column("feature_names", self.db.Text),
            self.db.Column("children", self.db.Text),
            self.db.Column("label_names", self.db.Text),
            self.db.Column("timestamp", self.db.DateTime),
            self.db.Column("endpoint_type", self.db.String(10)),
            self.db.Column("children_uids", self.db.Text),
            self.db.Column("drift_measures", self.db.Text),
            self.db.Column("drift_status", self.db.String(40)),
            self.db.Column("monitor_configuration", self.db.Text),
            self.db.Column("monitoring_feature_set_uri", self.db.String(255)),
            self.db.Column("latency_avg_5m", self.db.Float),
            self.db.Column("latency_avg_1h", self.db.Float),
            self.db.Column("predictions_per_second", self.db.Float),
            self.db.Column("predictions_count_5m", self.db.Float),
            self.db.Column("predictions_count_1h", self.db.Float),
            self.db.Column("first_request", self.db.String(40)),
            self.db.Column("last_request", self.db.String(40)),
            self.db.Column("error_count", self.db.Integer),
        )

    def delete_model_endpoints_resources(
            self, endpoints: mlrun.api.schemas.model_endpoints.ModelEndpointList
    ):
        """
        Delete all model endpoints resources in both SQL and the time series DB.

        :param endpoints: An object of ModelEndpointList which is literally a list of model endpoints along with some
                          metadata. To get a standard list of model endpoints use ModelEndpointList.endpoints.
        """

        # Delete model endpoint record from SQL table
        for endpoint in endpoints.endpoints:
            self.delete_model_endpoint(
                endpoint.metadata.uid,
            )