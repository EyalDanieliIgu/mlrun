# Copyright 2024 Iguazio
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
import http

import fastapi
import sqlalchemy.orm
from fastapi.concurrency import run_in_threadpool

import mlrun.common.runtimes.constants
import mlrun.common.schemas
import mlrun.common.schemas.alert as alert_objects
from mlrun import mlconf

import framework.api.deps
import framework.constants
import framework.db.base
import framework.db.session
import framework.db.sqldb.db
import framework.service
import framework.utils.auth.verifier
import framework.utils.clients.chief
import framework.utils.periodic
import framework.utils.singletons.db
import framework.utils.singletons.project_member
import framework.utils.time_window_tracker
import services.alerts.crud
import services.alerts.initial_data
import services.api.crud
from framework.db.session import close_session, create_session
from framework.routers import alerts, auth, healthz


class Service(framework.service.Service):
    # TODO: Change service name to alerts once they are fully separated - this allows to mount the application on the
    #  api Router without implementing tunneling
    service_name = "api"

    async def store_alert(
        self,
        request: fastapi.Request,
        project: str,
        name: str,
        alert_data: mlrun.common.schemas.AlertConfig,
        force_reset: bool = False,
        auth_info: mlrun.common.schemas.AuthInfo = None,
        db_session: sqlalchemy.orm.Session = None,
    ) -> mlrun.common.schemas.AlertConfig:
        await run_in_threadpool(
            framework.utils.singletons.project_member.get_project_member().ensure_project,
            db_session,
            project,
            auth_info=auth_info,
        )
        await framework.utils.auth.verifier.AuthVerifier().query_project_resource_permissions(
            mlrun.common.schemas.AuthorizationResourceTypes.alert,
            project,
            name,
            mlrun.common.schemas.AuthorizationAction.create,
            auth_info,
        )

        # TODO: Remove chief requirement when alerts is standalone
        if (
            mlrun.mlconf.httpdb.clusterization.role
            != mlrun.common.schemas.ClusterizationRole.chief
        ):
            chief_client = framework.utils.clients.chief.Client()
            data = await request.json()
            return await chief_client.store_alert(
                project=project, name=name, request=request, json=data
            )

        self._logger.debug("Storing alert", project=project, name=name)
        return await run_in_threadpool(
            services.alerts.crud.Alerts().store_alert,
            db_session,
            project,
            name,
            alert_data,
            force_reset,
        )

    async def get_alert(
        self,
        request: fastapi.Request,
        project: str,
        name: str,
        auth_info: mlrun.common.schemas.AuthInfo,
        db_session: sqlalchemy.orm.Session = None,
    ) -> mlrun.common.schemas.AlertConfig:
        await run_in_threadpool(
            framework.utils.singletons.project_member.get_project_member().ensure_project,
            db_session,
            project,
            auth_info=auth_info,
        )

        await framework.utils.auth.verifier.AuthVerifier().query_project_resource_permissions(
            mlrun.common.schemas.AuthorizationResourceTypes.alert,
            project,
            name,
            mlrun.common.schemas.AuthorizationAction.read,
            auth_info,
        )

        return await run_in_threadpool(
            services.alerts.crud.Alerts().get_enriched_alert, db_session, project, name
        )

    async def list_alerts(
        self,
        request: fastapi.Request,
        project: str,
        auth_info: mlrun.common.schemas.AuthInfo,
        db_session: sqlalchemy.orm.Session = None,
    ) -> list[mlrun.common.schemas.AlertConfig]:
        await run_in_threadpool(
            framework.utils.singletons.project_member.get_project_member().ensure_project,
            db_session,
            project,
            auth_info=auth_info,
        )
        allowed_project_names = (
            await services.api.crud.Projects().list_allowed_project_names(
                db_session, auth_info, project=project
            )
        )

        alerts = await run_in_threadpool(
            services.alerts.crud.Alerts().list_alerts,
            db_session,
            project=allowed_project_names,
        )

        alerts = await framework.utils.auth.verifier.AuthVerifier().filter_project_resources_by_permissions(
            mlrun.common.schemas.AuthorizationResourceTypes.alert,
            alerts,
            lambda alert: (
                alert.project,
                alert.name,
            ),
            auth_info,
        )

        return alerts

    async def delete_alert(
        self,
        request: fastapi.Request,
        project: str,
        name: str,
        auth_info: mlrun.common.schemas.AuthInfo,
        db_session: sqlalchemy.orm.Session = None,
    ):
        await run_in_threadpool(
            framework.utils.singletons.project_member.get_project_member().ensure_project,
            db_session,
            project,
            auth_info=auth_info,
        )

        await framework.utils.auth.verifier.AuthVerifier().query_project_resource_permissions(
            mlrun.common.schemas.AuthorizationResourceTypes.alert,
            project,
            name,
            mlrun.common.schemas.AuthorizationAction.delete,
            auth_info,
        )

        # TODO: Once alerts runs in its own pod - remove chief check
        if (
            mlrun.mlconf.httpdb.clusterization.role
            != mlrun.common.schemas.ClusterizationRole.chief
        ):
            chief_client = framework.utils.clients.chief.Client()
            return await chief_client.delete_alert(
                project=project, name=name, request=request
            )

        self._logger.debug("Deleting alert", project=project, name=name)

        await run_in_threadpool(
            services.alerts.crud.Alerts().delete_alert, db_session, project, name
        )

    async def reset_alert(
        self,
        request: fastapi.Request,
        project: str,
        name: str,
        auth_info: mlrun.common.schemas.AuthInfo,
        db_session: sqlalchemy.orm.Session = None,
    ):
        await run_in_threadpool(
            framework.utils.singletons.project_member.get_project_member().ensure_project,
            db_session,
            project,
            auth_info=auth_info,
        )
        await framework.utils.auth.verifier.AuthVerifier().query_project_resource_permissions(
            mlrun.common.schemas.AuthorizationResourceTypes.alert,
            project,
            name,
            mlrun.common.schemas.AuthorizationAction.update,
            auth_info,
        )

        # TODO: Once alerts runs in its own pod - remove chief check
        if (
            mlrun.mlconf.httpdb.clusterization.role
            != mlrun.common.schemas.ClusterizationRole.chief
        ):
            chief_client = framework.utils.clients.chief.Client()
            return await chief_client.reset_alert(
                project=project, name=name, request=request
            )

        self._logger.debug("Resetting alert", project=project, name=name)

        return await run_in_threadpool(
            services.alerts.crud.Alerts().reset_alert, db_session, project, name
        )

    async def post_event(
        self,
        request: fastapi.Request,
        project: str,
        name: str,
        event_data: mlrun.common.schemas.Event,
        auth_info: mlrun.common.schemas.AuthInfo,
        db_session: sqlalchemy.orm.Session = None,
    ):
        await run_in_threadpool(
            framework.utils.singletons.project_member.get_project_member().ensure_project,
            db_session,
            project,
            auth_info=auth_info,
        )
        await framework.utils.auth.verifier.AuthVerifier().query_project_resource_permissions(
            mlrun.common.schemas.AuthorizationResourceTypes.event,
            project,
            name,
            mlrun.common.schemas.AuthorizationAction.store,
            auth_info,
        )

        if mlrun.mlconf.alerts.mode == mlrun.common.schemas.alert.AlertsModes.disabled:
            self._logger.debug(
                "Alerts are disabled, skipping event processing",
                project=project,
                event_name=name,
            )
            return

        # TODO: Once alerts runs in its own pod - remove chief check
        if (
            mlrun.mlconf.httpdb.clusterization.role
            != mlrun.common.schemas.ClusterizationRole.chief
        ):
            data = await request.json()
            chief_client = framework.utils.clients.chief.Client()
            return await chief_client.set_event(
                project=project, name=name, request=request, json=data
            )

        self._logger.debug(
            "Got event", project=project, name=name, id=event_data.entity.ids[0]
        )

        if not services.alerts.crud.Events().is_valid_event(project, event_data):
            raise fastapi.HTTPException(status_code=http.HTTPStatus.BAD_REQUEST.value)

        await run_in_threadpool(
            services.alerts.crud.Events().process_event,
            db_session,
            event_data,
            name,
            project,
        )

    async def move_service_to_online(self):
        self._logger.info("Moving alerts to online")
        # TODO: Once alerts runs in its own pod - remove chief check
        if (
            mlconf.httpdb.clusterization.role
            == mlrun.common.schemas.ClusterizationRole.chief
        ):
            services.alerts.initial_data.update_default_configuration_data(self._logger)
            await self._start_periodic_functions()

    def _register_routes(self):
        # TODO: Resolve these dynamically from configuration
        alerts_v1_router = fastapi.APIRouter(
            dependencies=[fastapi.Depends(framework.api.deps.verify_api_state)]
        )
        alerts_v1_router.include_router(healthz.router, tags=["healthz"])
        alerts_v1_router.include_router(
            auth.router,
            tags=["auth"],
            dependencies=[fastapi.Depends(framework.api.deps.authenticate_request)],
        )
        alerts_v1_router.include_router(
            alerts.router,
            tags=["alerts"],
            dependencies=[fastapi.Depends(framework.api.deps.authenticate_request)],
        )
        self.app.include_router(
            alerts_v1_router, prefix=self.BASE_VERSIONED_SERVICE_PREFIX
        )

    async def _custom_setup_service(self):
        pass

    async def _start_periodic_functions(self):
        self._start_periodic_events_generation()

    def _start_periodic_events_generation(self):
        interval = int(mlconf.alerts.events_generation_interval)
        if interval > 0:
            self._logger.info("Starting events generation", interval=interval)
            framework.utils.periodic.run_function_periodically(
                interval,
                self._generate_events.__name__,
                False,
                self._generate_events,
            )

    async def _generate_events(self):
        db_session = await fastapi.concurrency.run_in_threadpool(create_session)
        try:
            await framework.utils.time_window_tracker.run_with_time_window_tracker(
                db_session=db_session,
                key=framework.utils.time_window_tracker.TimeWindowTrackerKeys.events_generation,
                max_window_size_seconds=int(
                    # TODO: This needs to be aligned with chief
                    mlconf.runtime_resources_deletion_grace_period
                ),
                ensure_window_update=False,
                callback=self._generate_event_on_failed_runs,
            )
        except Exception as exc:
            self._logger.warning(
                "Failed generating events. Ignoring",
                exc=mlrun.errors.err_to_str(exc),
            )
        finally:
            await fastapi.concurrency.run_in_threadpool(close_session, db_session)

    def _generate_event_on_failed_runs(
        self, db_session: sqlalchemy.orm.Session, last_update_time: datetime.datetime
    ):
        """
        Send an event on the runs that ended with error state since the last call to the function
        """
        db = framework.utils.singletons.db.get_db()
        runs = db.list_runs(
            db_session,
            project="*",
            states=[mlrun.common.runtimes.constants.RunStates.error],
            last_update_time_from=last_update_time,
        )

        for run in runs:
            project = run["metadata"]["project"]
            run_uid = run["metadata"]["uid"]
            run_name = run["metadata"]["name"]
            entity = mlrun.common.schemas.alert.EventEntities(
                kind=alert_objects.EventEntityKind.JOB,
                project=project,
                ids=[run_name],
            )
            event_value = {"uid": run_uid, "error": run["status"].get("error", None)}
            event_data = mlrun.common.schemas.Event(
                kind=alert_objects.EventKind.FAILED,
                entity=entity,
                value_dict=event_value,
            )

            services.alerts.crud.Events().process_event(
                session=db_session,
                event_data=event_data,
                event_name=alert_objects.EventKind.FAILED,
                project=project,
                validate_event=True,
            )