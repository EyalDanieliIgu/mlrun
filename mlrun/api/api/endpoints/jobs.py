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

from fastapi import (
    APIRouter,
    Depends,
)
from sqlalchemy.orm import Session
import mlrun.common.schemas
from mlrun.api.api import deps
from mlrun.api.api.endpoints.functions import process_model_monitoring_secret
from mlrun.model_monitoring import TrackingPolicy
import mlrun.api.crud.model_monitoring.deployment
router = APIRouter(prefix="/projects/{project}/jobs")
@router.post("/batch-monitoring")

async def deploy_monitoring_batch_job(
    project: str,
    auth_info: mlrun.common.schemas.AuthInfo = Depends(deps.authenticate_request),
    db_session: Session = Depends(deps.get_db_session),
    tracking_policy: dict = None
):
    print('[EYAL]: now in deploy monitoring batch job server side: ', tracking_policy)
    model_monitoring_access_key = None
    if not mlrun.mlconf.is_ce_mode():
        model_monitoring_access_key = process_model_monitoring_secret(
            db_session,
            project,
            mlrun.common.schemas.model_monitoring.ProjectSecretKeys.ACCESS_KEY,
        )
    if tracking_policy:
        # Convert to `TrackingPolicy` object as `fn.spec.tracking_policy` is provided as a dict
        tracking_policy = TrackingPolicy.from_dict(
            tracking_policy
        )
    else:
        # Initialize tracking policy with default values
        tracking_policy = TrackingPolicy()

    mlrun.api.crud.model_monitoring.deployment.MonitoringDeployment().deploy_model_monitoring_batch_processing(
        project=project,
        model_monitoring_access_key=model_monitoring_access_key,
        db_session=db_session,
        auth_info=auth_info,
        tracking_policy=tracking_policy

    )