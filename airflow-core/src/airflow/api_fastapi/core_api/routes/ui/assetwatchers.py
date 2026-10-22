# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, Any, cast

import structlog
from fastapi import Depends, HTTPException, status
from sqlalchemy import ColumnElement, and_, case, exists, func, select, true

from airflow.api_fastapi.common.asset_expression import redact_asset_expression
from airflow.api_fastapi.common.db.assets import generate_assets_with_last_event_query
from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
from airflow.api_fastapi.common.parameters import (
    QueryAssetDagIdPatternSearch,
    QueryAssetGroupPatternSearch,
    QueryAssetGroupPrefixPatternSearch,
    QueryAssetNamePatternSearch,
    QueryAssetNamePrefixPatternSearch,
    QueryHasEventsFilter,
    QueryLimit,
    QueryOffset,
    QueryUriExactMatch,
    QueryUriPatternSearch,
    QueryUriPrefixPatternSearch,
    RangeFilter,
    SortParam,
    datetime_range_filter_factory,
)
from airflow.api_fastapi.common.partition_helpers import load_partitioned_timetable
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.assets import AssetCollectionResponse, AssetResponse
from airflow.api_fastapi.core_api.datamodels.ui.assets import (
    NextRunAssetEventResponse,
    NextRunAssetsResponse,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.routes.public.assets import OnlyActiveFilter
from airflow.api_fastapi.core_api.security import (
    ReadableAssetsFilterDep,
    requires_access_asset,
    requires_access_asset_alias,
    requires_access_dag,
)
from airflow.models import DagModel
from airflow.api_fastapi.core_api.datamodels.assets import (
    AssetWatcherResponse
)
from airflow.models.asset import (
    AssetActive,
    AssetDagRunQueue,
    AssetEvent,
    AssetModel,
    AssetPartitionDagRun,
    AssetWatcherModel,
    DagScheduleAssetReference,
    PartitionedAssetKeyLog,
)

import contextlib
import textwrap
from collections.abc import Generator, Iterable

from fastapi import Depends, HTTPException, Request, status
from fastapi.responses import StreamingResponse
from itsdangerous import BadSignature, URLSafeSerializer
from pydantic import NonNegativeInt, PositiveInt
from sqlalchemy.orm import joinedload
from sqlalchemy.sql import select

from airflow.api_fastapi.common.dagbag import DagBagDep
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.headers import HeaderAcceptJsonOrNdjson
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.common.types import Mimetype
from airflow.api_fastapi.core_api.datamodels.log import ExternalLogUrlResponse, TaskInstancesLogResponse
from airflow.api_fastapi.core_api.datamodels.job import JobResponse
from airflow.api_fastapi.core_api.datamodels.trigger import TriggerResponse
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import DagAccessEntity, requires_access_dag
from airflow.api_fastapi.core_api.services.public.task_coordinates import TaskScopeDep
from airflow.configuration import conf
from airflow.exceptions import TaskNotFound
from airflow.models import TaskInstance, Trigger
from airflow.utils.log.log_reader import WatcherLogReader

if TYPE_CHECKING:
    from airflow.partition_mappers.base import RollupMapper

log = structlog.get_logger(logger_name=__name__)

assetwatchers_router = AirflowRouter(tags=["AssetWatcher"])

ndjson_example_response_for_get_log = {
    Mimetype.NDJSON: {
        "schema": {
            "type": "string",
            "example": textwrap.dedent(
                """\
    {"content": "content"}
    {"content": "content"}
    """
            ),
        }
    }
}


def _buffered_ndjson_stream(
    raw_stream: Iterable[str],
) -> Generator[str, None, None]:
    buf: list[str] = []
    for line in raw_stream:
        buf.append(line)
        if len(buf) >= _NDJSON_BATCH_SIZE:
            yield "".join(buf)
            buf.clear()
    if buf:
        yield "".join(buf)


@assetwatchers_router.get(
    "/assetwatchers/{assetwatcher_id}",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[
        Depends(requires_access_asset(method="GET")),
        Depends(requires_access_asset_alias(method="GET")),
    ],
)
def get_assetwatcher(
    assetwatcher_id: str,
    session: SessionDep,
) -> AssetWatcherResponse:

    watcher = session.execute(select(AssetWatcherModel)
          .where(AssetWatcherModel.name == assetwatcher_id)
          .options(joinedload(AssetWatcherModel.trigger).joinedload(Trigger.triggerer_job))
        ).scalar()

    if watcher is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Watcher not found")

    job = watcher.trigger.triggerer_job
    job = JobResponse.model_validate({
                                      "id": job.id,
                                      "dag_id": job.dag_id,
                                      "state": job.state,
                                      "job_type": job.job_type,
                                      "start_date": job.start_date,
                                      "end_date": job.end_date,
                                      "latest_heartbeat": job.latest_heartbeat,
                                      "executor_class": job.executor_class,
                                      "hostname": job.hostname,
                                      "unixname": job.unixname,
                                      "team_names": job.team_names,
                                      "bundle_names": job.bundle_names})

    trigger = watcher.trigger
    trigger = TriggerResponse.model_validate({
                                      "id": trigger.id,
                                      "classpath": trigger.classpath,
                                      "kwargs": "",
                                      "created_date": trigger.created_date,
                                      "queue": trigger.queue,
                                      "triggerer_id": trigger.triggerer_id,
                                      "triggerer_job": job
    })

    return AssetWatcherResponse.model_validate({ "name": watcher.name, "trigger_id": watcher.trigger_id, "created_date": trigger.created_date, "trigger": trigger })

@assetwatchers_router.get(
    "/assetwatchers/{assetwatcher_id}/log",
    responses={
        **create_openapi_http_exception_doc(
            [status.HTTP_400_BAD_REQUEST, status.HTTP_404_NOT_FOUND, status.HTTP_409_CONFLICT]
        ),
        status.HTTP_200_OK: {
            "description": "Successful Response",
            "content": ndjson_example_response_for_get_log,
        },
    },
    dependencies=[Depends(requires_access_dag("GET", DagAccessEntity.TASK_LOGS))],
    response_model=TaskInstancesLogResponse,
    response_model_exclude_unset=True,
)
def get_assetwatcher_log(
    assetwatcher_id: str,
    scope: TaskScopeDep,
    accept: HeaderAcceptJsonOrNdjson,
    request: Request,
    session: SessionDep,
    full_content: bool = False,
    token: str | None = None,
):
    """Get logs for a specific task instance."""
    if not token:
        metadata = {}
    else:
        try:
            metadata = URLSafeSerializer(request.app.state.secret_key).loads(token)
        except BadSignature:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST, "Bad Signature. Please use only the tokens provided by the API."
            )

    if metadata.get("download_logs") and metadata["download_logs"]:
        full_content = True

    metadata["download_logs"] = full_content

    task_log_reader = WatcherLogReader()

    if not task_log_reader.supports_read:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Task log handler does not support read logs.")

    watcher = session.execute(select(AssetWatcherModel)
          .where(AssetWatcherModel.name == assetwatcher_id)
          .options(joinedload(AssetWatcherModel.trigger).joinedload(Trigger.triggerer_job))
          ).scalar()

    if watcher is None:
        metadata["end_of_log"] = True
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Watcher not found")

    if accept == Mimetype.NDJSON:  # only specified application/x-ndjson will return streaming response
        # LogMetadata(TypedDict) is used as type annotation for log_reader; added ignore to suppress mypy error
        raw_stream = task_log_reader.read_log_stream(watcher, metadata)  # type: ignore[arg-type]
        log_stream = _buffered_ndjson_stream(raw_stream)
        headers = None
        if not metadata.get("end_of_log", False):
            headers = {
                "Airflow-Continuation-Token": URLSafeSerializer(request.app.state.secret_key).dumps(metadata)
            }
        return StreamingResponse(media_type="application/x-ndjson", content=log_stream, headers=headers)

    # application/json, or something else we don't understand.
    # Return JSON format, which will be more easily for users to debug.

    # LogMetadata(TypedDict) is used as type annotation for log_reader; added ignore to suppress mypy error
    structured_log_stream, out_metadata = task_log_reader.read_log_chunks(watcher, metadata)  # type: ignore[arg-type]
    encoded_token = None
    if not out_metadata.get("end_of_log", False):
        encoded_token = URLSafeSerializer(request.app.state.secret_key).dumps(out_metadata)
    return TaskInstancesLogResponse.model_construct(
        continuation_token=encoded_token, content=list(structured_log_stream)
    )
