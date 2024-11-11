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

import datetime
from typing import Optional

import sqlalchemy.orm

import mlrun.common.formatters
import mlrun.common.schemas
import mlrun.common.types
import mlrun.config
import mlrun.errors
import mlrun.utils.singleton

import services.api.api.utils
import services.api.runtime_handlers
import services.api.utils.singletons.db


class Functions(
    metaclass=mlrun.utils.singleton.Singleton,
):
    def store_function(
        self,
        db_session: sqlalchemy.orm.Session,
        function: dict,
        name: str,
        project: Optional[str] = None,
        tag: str = "",
        versioned: bool = False,
        auth_info: mlrun.common.schemas.AuthInfo = None,
    ) -> str:
        project = project or mlrun.mlconf.default_project
        if auth_info:
            function_obj = mlrun.new_function(
                name=name, project=project, runtime=function, tag=tag
            )
            # not raising exception if no access key was provided as the store of the function can be part of
            # intermediate steps or temporary objects which might not be executed at any phase and therefore we don't
            # want to enrich if user didn't requested.
            # (The way user will request to generate is by passing $generate in the metadata.credentials.access_key)
            services.api.api.utils.ensure_function_auth_and_sensitive_data_is_masked(
                function_obj, auth_info, allow_empty_access_key=True
            )
            function = function_obj.to_dict()

        return services.api.utils.singletons.db.get_db().store_function(
            db_session,
            function,
            name,
            project,
            tag,
            versioned,
        )

    def get_function(
        self,
        db_session: sqlalchemy.orm.Session,
        name: str,
        project: Optional[str] = None,
        tag: str = "",
        hash_key: str = "",
        format_: Optional[str] = None,
    ) -> dict:
        project = project or mlrun.mlconf.default_project
        return services.api.utils.singletons.db.get_db().get_function(
            db_session, name, project, tag, hash_key, format_
        )

    def delete_function(
        self,
        db_session: sqlalchemy.orm.Session,
        project: str,
        name: str,
    ):
        return services.api.utils.singletons.db.get_db().delete_function(
            db_session, project, name
        )

    def list_functions(
        self,
        db_session: sqlalchemy.orm.Session,
        project: Optional[str] = None,
        name: Optional[str] = None,
        tag: Optional[str] = None,
        labels: Optional[list[str]] = None,
        hash_key: Optional[str] = None,
        page: Optional[int] = None,
        page_size: Optional[int] = None,
        format_: mlrun.common.formatters.FunctionFormat = None,
        since: Optional[datetime.datetime] = None,
        until: Optional[datetime.datetime] = None,
    ) -> list:
        project = project or mlrun.mlconf.default_project
        if labels is None:
            labels = []
        return services.api.utils.singletons.db.get_db().list_functions(
            session=db_session,
            name=name,
            project=project,
            tag=tag,
            labels=labels,
            hash_key=hash_key,
            format_=format_,
            since=since,
            until=until,
            page=page,
            page_size=page_size,
        )

    def get_function_status(
        self,
        kind,
        selector,
    ):
        resource = services.api.runtime_handlers.runtime_resources_map.get(kind)
        if "status" not in resource:
            raise mlrun.errors.MLRunBadRequestError(
                reason="Runtime error: 'status' not supported by this runtime"
            )

        return resource["status"](selector)

    def start_function(self, function, client_version=None, client_python_version=None):
        resource = services.api.runtime_handlers.runtime_resources_map.get(
            function.kind
        )
        if "start" not in resource:
            raise mlrun.errors.MLRunBadRequestError(
                reason="Runtime error: 'start' not supported by this runtime"
            )

        resource["start"](
            function,
            client_version=client_version,
            client_python_version=client_python_version,
        )
        function.save(versioned=False)

    def update_function(
        self,
        db_session: sqlalchemy.orm.Session,
        function,
        project,
        updates: dict,
    ):
        return services.api.utils.singletons.db.get_db().update_function(
            session=db_session,
            name=function["metadata"]["name"],
            tag=function["metadata"]["tag"],
            hash_key=function.get("metadata", {}).get("hash"),
            project=project,
            updates=updates,
        )

    def add_function_external_invocation_url(
        self,
        db_session: sqlalchemy.orm.Session,
        function_uri: str,
        project: str,
        invocation_url: str,
    ):
        _, function_name, tag, hash_key = (
            mlrun.common.helpers.parse_versioned_object_uri(function_uri)
        )
        services.api.utils.singletons.db.get_db().update_function_external_invocation_url(
            session=db_session,
            name=function_name,
            url=invocation_url,
            project=project,
            tag=tag,
            hash_key=hash_key,
            operation=mlrun.common.types.Operation.ADD,
        )

    def delete_function_external_invocation_url(
        self,
        db_session: sqlalchemy.orm.Session,
        function_uri: str,
        project: str,
        invocation_url: str,
    ):
        _, function_name, tag, hash_key = (
            mlrun.common.helpers.parse_versioned_object_uri(function_uri)
        )
        services.api.utils.singletons.db.get_db().update_function_external_invocation_url(
            session=db_session,
            name=function_name,
            url=invocation_url,
            project=project,
            tag=tag,
            hash_key=hash_key,
            operation=mlrun.common.types.Operation.REMOVE,
        )