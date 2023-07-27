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
import time
import typing
from mlrun.artifacts import Artifact
import mlrun
import datetime
import mlrun.common.schemas
import mlrun.common.helpers
import mlrun.model_monitoring.model_endpoint
import mlrun.feature_store
import pandas as pd
import numpy as np
import hashlib
from mlrun.data_types.infer import InferOptions, get_df_stats
from .model_monitoring_batch import  VirtualDrift
from .features_drift_table import FeaturesDriftTablePlot


import json
# A union of all supported dataset types:
DatasetType = typing.Union[mlrun.DataItem, list, dict, pd.DataFrame, pd.Series, np.ndarray, typing.Any]

def get_or_create_model_endpoint(context: mlrun.MLClientCtx, endpoint_id: str, model_path: str, model_name: str, df_to_target: pd.DataFrame, sample_set_statistics,
                                 drift_threshold, possible_drift_threshold, inf_capping, artifacts_tag, trigger_monitoring_job,  default_batch_image="quay.io/eyaligu/mlrun-api:fix-batch-inf"):
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

    if trigger_monitoring_job:
        res = trigger_drift_batch_job(project=context.project, default_batch_image=default_batch_image,
                                model_endpoints_ids=[endpoint_id])


    perform_drift_analysis(
        context=context,
        sample_set_statistics= sample_set_statistics,
        inputs=df_to_target,
        drift_threshold=drift_threshold,
        possible_drift_threshold=possible_drift_threshold,
        inf_capping= inf_capping,
        artifacts_tag=artifacts_tag,
        endpoint_id=endpoint_id,
        db_session=db,
    )




def _generate_model_endpoint(context: mlrun.MLClientCtx, db, endpoint_id: str, model_path: str,model_name: str, sample_set_statistics):

    model_endpoint = mlrun.model_monitoring.model_endpoint.ModelEndpoint()
    model_endpoint.metadata.project = context.project
    model_endpoint.metadata.uid = endpoint_id
    model_endpoint.spec.model_uri = model_path
    (_,
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


def trigger_drift_batch_job(project: str,   default_batch_image="mlrun/mlrun", model_endpoints_ids: typing.List[str] = None, batch_intervals_dict: dict = None):

    if not model_endpoints_ids:
        raise mlrun.errors.MLRunNotFoundError(
            f"No model endpoints provided",
        )

    db = mlrun.get_run_db()

    res = db.deploy_monitoring_batch_job(project=project, default_batch_image=default_batch_image, trigger_job=True, model_endpoints_ids=model_endpoints_ids, batch_intervals_dict=batch_intervals_dict)

    job_params = _generate_job_params(model_endpoints_ids=model_endpoints_ids,
                                      batch_intervals_dict=batch_intervals_dict)

    batch_function = mlrun.new_function(runtime=res)
    batch_function.run(name="model-monitoring-batch", params=job_params, watch=True)

    return res

def _generate_job_params(model_endpoints_ids: typing.List[str],
                         batch_intervals_dict: dict = None):
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


def perform_drift_analysis(
    context: mlrun.MLClientCtx,
    sample_set_statistics: dict,
    drift_threshold: float,
    possible_drift_threshold: float,
    inf_capping: float,
db_session,
    artifacts_tag: str = "",
        endpoint_id: str = "",

):
    """
    Perform drift analysis, producing the drift table artifact for logging post prediction.

    :param sample_set_statistics:    The statistics of the sample set logged along a model.
    :param inputs:                   Input dataset to perform the drift calculation on.
    :param drift_threshold:          The threshold of which to mark drifts.
    :param possible_drift_threshold: The threshold of which to mark possible drifts.
    :param inf_capping:              The value to set for when it reached infinity.

    :returns: A tuple of
              [0] = An MLRun artifact holding the HTML code of the drift table plot.
              [1] = An MLRun artifact holding the metric per feature dictionary.
              [2] = Results to log the final analysis outcome.
    """


    model_endpoint = db_session.get_model_endpoint(project=context.project, endpoint_id=endpoint_id)

    metrics = model_endpoint.status.drift_measures
    inputs_statistics = model_endpoint.status.current_stats

    inputs_statistics.pop('timestamp', None)

    virtual_drift = VirtualDrift(inf_capping=inf_capping)

    drift_results = virtual_drift.check_for_drift_per_feature(
        metrics_results_dictionary=metrics,
        possible_drift_threshold=possible_drift_threshold,
        drift_detected_threshold=drift_threshold,
    )
    print('[EYAL]: metrics: ', metrics)

    # Plot:
    html_plot = FeaturesDriftTablePlot().produce(
        features=list(inputs_statistics.keys()),
        sample_set_statistics=sample_set_statistics,
        inputs_statistics=inputs_statistics,
        metrics=metrics,
        drift_results=drift_results,
    )

    # Prepare metrics per feature dictionary:
    metrics_per_feature = {
        feature: _get_drift_result(
            tvd=metric_dictionary["tvd"],
            hellinger=metric_dictionary["hellinger"],
            threshold=drift_threshold,
        )[1]
        for feature, metric_dictionary in metrics.items()
        if isinstance(metric_dictionary, dict)
    }
    print('[EYAL]: metrics per feature: ', metrics_per_feature)
    # Calculate the final analysis result:
    drift_status, drift_metric = _get_drift_result(
        tvd=metrics["tvd_mean"],
        hellinger=metrics["hellinger_mean"],
        threshold=drift_threshold,
    )

    _log_drift_artifacts(context, html_plot, metrics_per_feature, drift_status, drift_metric, artifacts_tag)


def _log_drift_artifacts(context,
html_plot, metrics_per_feature, drift_status, drift_metric,artifacts_tag
):


    context.log_artifact(Artifact(body=html_plot, format="html", key="drift_table_plot"))
    context.log_artifact(Artifact(
        body=json.dumps(metrics_per_feature),
        format="json",
        key="features_drift_results",
    ))
    context.log_results(results={"drift_status": drift_status, "drift_metric": drift_metric})

def _get_drift_result(
    tvd: float,
    hellinger: float,
    threshold: float,
) -> typing.Tuple[bool, float]:
    """
    Calculate the drift result by the following equation: (tvd + hellinger) / 2

    :param tvd:       The feature's TVD value.
    :param hellinger: The feature's Hellinger value.
    :param threshold: The threshold from which the value is considered a drift.

    :returns: A tuple of:
              [0] = Boolean value as the drift status.
              [1] = The result.
    """
    result = (tvd + hellinger) / 2
    if result >= threshold:
        return True, result
    return False, result

def log_result(context, result_set_name, result_set, artifacts_tag, batch_id):
    # Log the result set:
    context.logger.info(f"Logging result set (x | prediction)...")
    context.log_dataset(
        key=result_set_name,
        df=result_set,
        db_key=result_set_name,
        tag=artifacts_tag,
    )
    # Log the batch ID:
    if batch_id is None:
        batch_id = hashlib.sha224(str(datetime.datetime.now()).encode()).hexdigest()
    context.log_result(
        key="batch_id",
        value=batch_id,
    )