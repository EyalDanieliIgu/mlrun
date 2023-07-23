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
    model_endpoint.spec.monitoring_mode = mlrun.common.schemas.model_monitoring.ModelMonitoringMode.enabled.value
    model_endpoint.status.feature_stats = sample_set_statistics
    db.create_model_endpoint(project=context.project, endpoint_id=endpoint_id, model_endpoint=model_endpoint)

    return db.get_model_endpoint(project=context.project, endpoint_id=endpoint_id)

def trigger_drift_batch_job(project: str, name="model-monitoring-batch", with_schedule=False, default_batch_image="mlrun/mlrun", model_endpoints_ids: typing.List[str] = None, log_artifacts: bool = True, artifacts_tag: str = "", batch_intervals_dict: dict = None):

    if not model_endpoints_ids:
        raise mlrun.errors.MLRunNotFoundError(
            f"No model endpoints provided",
        )

    db = mlrun.get_run_db()
    try:
        function_dict = db.get_function(project=project, name=name)

    except mlrun.errors.MLRunNotFoundError:
        tracking_policy = mlrun.model_monitoring.TrackingPolicy(with_schedule=with_schedule, default_batch_image=default_batch_image)
        db.deploy_monitoring_batch_job(project=project, tracking_policy=tracking_policy)
        function_dict = db.get_function(project=project, name=name)
    function_runtime = mlrun.new_function(runtime=function_dict)
    job_params = _generate_job_params(model_endpoints_ids=model_endpoints_ids, log_artifacts=log_artifacts, artifacts_tag=artifacts_tag, batch_intervals_dict=batch_intervals_dict)
    function_runtime.run(name=name, params=job_params)

def _generate_job_params(model_endpoints_ids: typing.List[str], log_artifacts: bool = True, artifacts_tag: str = "", batch_intervals_dict: dict = None):
    if not batch_intervals_dict:
        # Generate default batch intervals dict
        batch_intervals_dict = {"minutes": 0, "hours": 2, "days": 0}

    return {
        "model_endpoints": model_endpoints_ids,
        "log_artifacts": log_artifacts,
        "artifacts_tag": artifacts_tag,
        "batch_intervals_dict": batch_intervals_dict
    }
