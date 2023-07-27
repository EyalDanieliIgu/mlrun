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
from typing import List

from fastapi import (
    APIRouter,
    Depends,
Query,
)
from sqlalchemy.orm import Session
import mlrun.common.schemas
from mlrun.api.api import deps
from mlrun.api.api.endpoints.functions import process_model_monitoring_secret
from mlrun.model_monitoring import TrackingPolicy
import mlrun.api.crud.model_monitoring.deployment
router = APIRouter(prefix="/projects/{project}/jobs")
@router.post("/batch-monitoring")
def deploy_monitoring_batch_job(
    project: str,
    auth_info: mlrun.common.schemas.AuthInfo = Depends(deps.authenticate_request),
    db_session: Session = Depends(deps.get_db_session),
    # tracking_policy: dict = None,
    default_batch_image: str = "mlrun/mlrun",
    with_schedule: bool = False,
    trigger_job: bool = False,
    model_endpoints_ids: List[str] = Query(None, alias="model_endpoint_id"),
    batch_intervals_dict: dict = None
):
    print('[EYAL]: now in deploy monitoring batch job server side! ')


    model_monitoring_access_key = None
    if not mlrun.mlconf.is_ce_mode():
        model_monitoring_access_key = process_model_monitoring_secret(
            db_session,
            project,
            mlrun.common.schemas.model_monitoring.ProjectSecretKeys.ACCESS_KEY,
        )
    # if tracking_policy:
    #     # Convert to `TrackingPolicy` object as `fn.spec.tracking_policy` is provided as a dict
    #     tracking_policy = TrackingPolicy.from_dict(
    #         tracking_policy
    #     )
    # else:
    #     # Initialize tracking policy with default values
    #     tracking_policy = TrackingPolicy()
    tracking_policy = TrackingPolicy(default_batch_image=default_batch_image)
    batch_function = mlrun.api.crud.model_monitoring.deployment.MonitoringDeployment().deploy_model_monitoring_batch_processing(
        project=project,
        model_monitoring_access_key=model_monitoring_access_key,
        db_session=db_session,
        auth_info=auth_info,
        tracking_policy=tracking_policy,
    with_schedule=with_schedule,

    )
    print('[EYAL]: batch_function.to_dict(): ', batch_function.to_dict())
    return {
        "func": batch_function.to_dict(),
    }


