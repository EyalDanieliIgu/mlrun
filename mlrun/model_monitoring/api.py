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
#
import typing

import mlrun
import datetime
import mlrun.common.schemas
import mlrun.common.helpers
import mlrun.model_monitoring.model_endpoint
import mlrun.feature_store
import pandas as pd
import numpy as np
from mlrun.data_types.infer import InferOptions, get_df_stats

# A union of all supported dataset types:
DatasetType = typing.Union[mlrun.DataItem, list, dict, pd.DataFrame, pd.Series, np.ndarray, typing.Any]

def get_or_create_model_endpoint(context: mlrun.MLClientCtx, endpoint_id: str, model_path: str, model_name: str, df_to_target: pd.DataFrame, sample_set_statistics):
    db = mlrun.get_run_db()
    try:
        model_endpoint = db.get_model_endpoint(project=context.project, endpoint_id=endpoint_id)
        model_endpoint.status.last_request = datetime.datetime.now()

    except mlrun.errors.MLRunNotFoundError:
        model_endpoint = _generate_model_endpoint(context=context, db=db,  endpoint_id=endpoint_id, model_path=model_path,
                                 model_name=model_name,sample_set_statistics=sample_set_statistics)

    monitoring_feature_set = mlrun.feature_store.get_feature_set(uri=model_endpoint.status.monitoring_feature_set_uri)
    df_to_target[mlrun.common.schemas.model_monitoring.EventFieldType.TIMESTAMP] = datetime.datetime.now()
    df_to_target[mlrun.common.schemas.model_monitoring.EventFieldType.ENDPOINT_ID] = endpoint_id
    df_to_target.set_index(mlrun.common.schemas.model_monitoring.EventFieldType.ENDPOINT_ID, inplace=True)
    mlrun.feature_store.ingest(featureset=monitoring_feature_set, source=df_to_target, overwrite=False)


def _generate_model_endpoint(context: mlrun.MLClientCtx, db, endpoint_id: str, model_path: str,model_name: str, sample_set_statistics):
    print("[EYAL]: Creating a new model endpoint record")

    model_endpoint = mlrun.model_monitoring.model_endpoint.ModelEndpoint()
    model_endpoint.metadata.project = context.project
    model_endpoint.metadata.uid = endpoint_id
    model_endpoint.spec.model_uri = model_path

    print('[EYAL]: function uri: ', context.to_dict()['spec']['function'])
    (   _,
        _,
        _,
        function_hash,
    ) = mlrun.common.helpers.parse_versioned_object_uri(context.to_dict()['spec']['function'])



    model_endpoint.spec.function_uri = context.project+"/"+function_hash
    model_endpoint.spec.model = model_name
    model_endpoint.spec.model_class = 'drift-analysis'
    model_endpoint.status.first_request = datetime.datetime.now()
    model_endpoint.status.last_request = datetime.datetime.now()
    model_endpoint.spec.monitoring_mode = mlrun.common.schemas.model_monitoring.ModelMonitoringMode.enabled.value if sample_set_statistics else mlrun.common.schemas.model_monitoring.ModelMonitoringMode.enabled.disabled
    model_endpoint.status.feature_stats = sample_set_statistics
    db.create_model_endpoint(project=context.project, endpoint_id=endpoint_id, model_endpoint=model_endpoint)

    return db.get_model_endpoint(project=context.project, endpoint_id=endpoint_id)

## move below code to crud side
def trigger_drift_batch_job(project: str, name="model-monitoring-batch", with_schedule=False, default_batch_image="mlrun/mlrun", model_endpoints_ids: typing.List[str] = None, log_artifacts: bool = True, artifacts_tag: str = "", batch_intervals_dict: dict = None):

    if not model_endpoints_ids:
        raise mlrun.errors.MLRunNotFoundError(
            f"No model endpoints provided",
        )

    db = mlrun.get_run_db()
    try:
        function_dict = db.get_function(project=project, name=name)

    except mlrun.errors.MLRunNotFoundError:
        tracking_policy = mlrun.model_monitoring.TrackingPolicy(default_batch_image=default_batch_image)
        db.deploy_monitoring_batch_job(project=project, tracking_policy=tracking_policy)
        function_dict = db.get_function(project=project, name=name)
    function_runtime = mlrun.new_function(runtime=function_dict)
    job_params = _generate_job_params(model_endpoints_ids=model_endpoints_ids, batch_intervals_dict=batch_intervals_dict)
    function_runtime.run(name=name, params=job_params)

def _generate_job_params(model_endpoints_ids: typing.List[str], log_artifacts: bool = True, artifacts_tag: str = "", batch_intervals_dict: dict = None):
    if not batch_intervals_dict:
        # Generate default batch intervals dict
        batch_intervals_dict = {"minutes": 0, "hours": 2, "days": 0}

    return {
        "model_endpoints": model_endpoints_ids,
        "batch_intervals_dict": batch_intervals_dict
    }

def get_sample_set_statistics(
    sample_set: DatasetType = None, model_artifact_feature_stats: dict = None
) -> dict:
    """
    Get the sample set statistics either from the given sample set or the statistics logged with the model while
    favoring the given sample set.

    :param sample_set:                   A sample dataset to give to compare the inputs in the drift analysis.
    :param model_artifact_feature_stats: The `feature_stats` attribute in the spec of the model artifact, where the
                                         original sample set statistics of the model was used.

    :returns: The sample set statistics.

    raises MLRunInvalidArgumentError: If no sample set or statistics were given.
    """
    # Check if a sample set was provided:
    if sample_set is None:
        # Check if the model was logged with a sample set:
        if model_artifact_feature_stats is None:
            raise mlrun.errors.MLRunInvalidArgumentError(
                "Cannot perform drift analysis as there is no sample set to compare to. The model artifact was not "
                "logged with a sample set and `sample_set` was not provided to the function."
            )
        # Return the statistics logged with the model:
        return model_artifact_feature_stats

    # Turn the DataItem to DataFrame:
    if isinstance(sample_set, mlrun.DataItem):
        sample_set, _ = read_dataset_as_dataframe(dataset=sample_set)

    # Return the sample set statistics:
    return get_df_stats(df=sample_set, options=InferOptions.Histogram)

def read_dataset_as_dataframe(
    dataset: DatasetType,
    label_columns: typing.Union[str, typing.List[str]] = None,
    drop_columns: typing.Union[str, typing.List[str], int, typing.List[int]] = None,
) -> typing.Tuple[pd.DataFrame, typing.List[str]]:
    """
    Parse the given dataset into a DataFrame and drop the columns accordingly. In addition, the label columns will be
    parsed and validated as well.

    :param dataset:       The dataset to train the model on.
                          Can be either a list of lists, dict, URI or a FeatureVector.
    :param label_columns: The target label(s) of the column(s) in the dataset. for Regression or
                          Classification tasks.
    :param drop_columns:  ``str`` / ``int`` or a list of ``str`` / ``int`` that represent the column names / indices to
                          drop.

    :returns: A tuple of:
              [0] = The parsed dataset as a DataFrame
              [1] = Label columns.

    raises MLRunInvalidArgumentError: If the `drop_columns` are not matching the dataset or unsupported dataset type.
    """
    # Turn the `drop labels` into a list if given:
    if drop_columns is not None:
        if not isinstance(drop_columns, list):
            drop_columns = [drop_columns]

    # Check if the dataset is in fact a Feature Vector:
    if dataset.meta and dataset.meta.kind == mlrun.common.schemas.ObjectKind.feature_vector:
        # Try to get the label columns if not provided:
        if label_columns is None:
            label_columns = dataset.meta.status.label_column
        # Get the features and parse to DataFrame:
        dataset = mlrun.feature_store.get_offline_features(
            dataset.meta.uri, drop_columns=drop_columns
        ).to_dataframe()
    else:
        # Parse to DataFrame according to the dataset's type:
        if isinstance(dataset, (list, np.ndarray)):
            # Parse the list / numpy array into a DataFrame:
            dataset = pd.DataFrame(dataset)
            # Validate the `drop_columns` is given as integers:
            if drop_columns and not all(isinstance(col, int) for col in drop_columns):
                raise mlrun.errors.MLRunInvalidArgumentError(
                    "`drop_columns` must be an integer / list of integers if provided as a list."
                )
        elif isinstance(dataset, mlrun.DataItem):
            # Turn the DataITem to DataFrame:
            dataset = dataset.as_df()
        else:
            # Parse the object (should be a pd.DataFrame / pd.Series, dictionary) into a DataFrame:
            try:
                dataset = pd.DataFrame(dataset)
            except ValueError as e:
                raise mlrun.errors.MLRunInvalidArgumentError(
                    f"Could not parse the given dataset of type {type(dataset)} into a pandas DataFrame. "
                    f"Received the following error: {e}"
                )
        # Drop columns if needed:
        if drop_columns:
            dataset.drop(drop_columns, axis=1, inplace=True)

    # Turn the `label_columns` into a list by default:
    if label_columns is None:
        label_columns = []
    elif isinstance(label_columns, (str, int)):
        label_columns = [label_columns]

    return dataset, label_columns