from __future__ import annotations

import heapq
import io
import logging
import os
from collections.abc import Generator, Iterator
from contextlib import suppress
from datetime import datetime
from enum import Enum
from itertools import chain, islice
from pathlib import Path
from types import GeneratorType
from typing import IO, TYPE_CHECKING, NotRequired, TypedDict, cast
from urllib.parse import urljoin

import pendulum
from pydantic import BaseModel, ConfigDict, ValidationError

from airflow.configuration import conf
from airflow.executors.executor_loader import ExecutorLoader
from airflow.utils.log.log_stream_accumulator import LogStreamAccumulator
from airflow.utils.log.logging_mixin import SetContextPropagate
from airflow.utils.log.non_caching_file_handler import NonCachingRotatingFileHandler
from airflow.utils.state import State

from airflow.models.asset import AssetWatcherModel

from airflow.utils.log.file_task_handler import (
    StructuredLogMessage,
    LogMetadata,
    _stream_lines_by_chunk,
    _is_logs_stream_like,
    _log_stream_to_parsed_log_stream,
    _get_compatible_log_stream,
    _interleave_logs,
    _fetch_logs_from_service,
    HEAP_DUMP_SIZE
    )

if TYPE_CHECKING:
    from typing import TypeAlias

    from requests import Response
    from sqlalchemy.orm import Session

    from airflow._shared.logging.remote import (
        LogMessages,
        LogResponse,
        LogSourceInfo,
        RawLogStream,
        StreamingLogResponse
    )

    from airflow.utils.log.file_task_handler import (
      LogHandlerOutputStream
    )

logger = logging.getLogger(__name__)


class FileWatcherHandler(logging.Handler):
    def _render_filename(self, assetWatcher: AssetWatcherModel):
        return f"trigger-{assetWatcher.trigger_id}.log"

    def _get_log_retrieval_url(
        self,
        assetWatcher: AssetWatcherModel,
        log_relative_path: str,
    ) -> tuple[str | None, str | None]:
        """Given TI, generate URL with which to fetch logs from service log server."""
        if not assetWatcher.trigger.triggerer_job:
            raise RuntimeError("Could not build triggerer log URL; no triggerer job.")
        config_key = "trigger_log_server_port"
        deprecated_config_key = "triggerer_log_server_port"
        if (
            conf.get("logging", config_key, fallback=None) is None
            and conf.get("logging", deprecated_config_key, fallback=None) is not None
        ):
            logger.warning(
                "The [logging] %s option is deprecated. Please use [logging] %s instead.",
                deprecated_config_key,
                config_key,
            )
            config_key = deprecated_config_key
        config_default = 8794
        hostname = assetWatcher.trigger.triggerer_job.hostname

        if not hostname:
            return None, None

        return (
            urljoin(
                f"http://{hostname}:{conf.get('logging', config_key, fallback=config_default)}/log/",
                log_relative_path,
            ),
            log_relative_path,
        )

    def _read_from_logs_server(
        self,
        assetWatcher: AssetWatcherModel,
        worker_log_rel_path: str,
    ) -> StreamingLogResponse:
        sources: LogSourceInfo = []
        log_streams: list[RawLogStream] = []
        try:
            url, rel_path = self._get_log_retrieval_url(assetWatcher, worker_log_rel_path)
            if not url or not rel_path:
                sources.append(
                    f"Could not read served logs: Hostname not available for Trigger"
                    f"Please check your `hostname_callable` configuration."
                )
                return sources, log_streams
            response = _fetch_logs_from_service(url, rel_path)
            if response.status_code == 403:
                sources.append(
                    "!!!! Please make sure that all your Airflow components (e.g. "
                    "schedulers, api-servers, dag-processors, workers and triggerer) have "
                    "the same 'secret_key' configured in '[api]' section and "
                    "time is synchronized on all your machines (for example with ntpd)\n"
                    "See more at https://airflow.apache.org/docs/apache-airflow/"
                    "stable/configurations-ref.html#secret-key"
                )
            elif response.status_code == 404:
                # Log file not found on the worker's log server.
                # This typically happens when a task retried on a different worker
                # and the original worker's logs are no longer accessible.
                # Fall back to local filesystem read if available.
                worker_log_full_path = Path(self.local_base, worker_log_rel_path)
                fallback_sources, fallback_streams = self._read_from_local(worker_log_full_path)
                if fallback_sources:
                    sources.extend(fallback_sources)
                    log_streams.extend(fallback_streams)
                else:
                    sources.append(
                        f"Log file not found on worker '{ti.hostname}'. "
                        f"This attempt may have run on a different worker whose logs "
                        f"are no longer accessible. "
                        f"Consider configuring remote logging (S3, GCS, etc.) for log persistence."
                    )
            else:
                # Check if the resource was properly fetched
                response.raise_for_status()

                if int(response.headers.get("Content-Length", 0)) > 0:
                    sources.append(url)
                    log_streams.append(
                        _stream_lines_by_chunk(io.TextIOWrapper(cast("IO[bytes]", response.raw)))
                    )
        except Exception as e:
            from requests.exceptions import InvalidURL

            if isinstance(e, InvalidURL):
                sources.append(self.inherits_from_empty_operator_log_message)
            else:
                sources.append(f"Could not read served logs: {e}")
                logger.exception("Could not read served logs")
        return sources, log_streams

    def _read(self, assetWatcher: AssetWatcherModel, metadata: LogMetadata | None = None,):
        """
        Template method that contains custom logic of reading logs given the try_number.

        :param ti: task instance record
        :param try_number: current try_number to read log from
        :param metadata: log metadata,
                         can be used for steaming log reading and auto-tailing.
                         Following attributes are used:
                         log_pos: (absolute) Char position to which the log
                                  which was retrieved in previous calls, this
                                  part will be skipped and only following test
                                  returned to be added to tail.
        :return: log message as a string and metadata.
                 Following attributes are used in metadata:
                 end_of_log: Boolean, True if end of log is reached or False
                             if further calls might get more log text.
                             This is determined by the status of the TaskInstance
                 log_pos: (absolute) Char position to which the log is retrieved
        """
        # Task instance here might be different from task instance when
        # initializing the handler. Thus explicitly getting log location
        # is needed to get correct log path.
        worker_log_rel_path = self._render_filename(assetWatcher)
        sources: LogSourceInfo = []
        source_list: list[str] = []
        remote_logs: list[RawLogStream] = []
        local_logs: list[RawLogStream] = []
        executor_logs: list[RawLogStream] = []
        served_logs: list[RawLogStream] = []
        with suppress(NotImplementedError):
            sources, logs = self._read_remote_logs(assetWatcher, metadata)
            if not logs:
                remote_logs = []
            elif isinstance(logs, list) and isinstance(logs[0], str):
                # If the logs are in legacy format, convert them to a generator of log lines
                remote_logs = [
                    # We don't need to use the log_pos here, as we are using the metadata to track the position
                    _get_compatible_log_stream(logs)
                ]
            elif isinstance(logs, list) and _is_logs_stream_like(logs[0]):
                # If the logs are already in a stream-like format, we can use them directly
                remote_logs = cast("list[RawLogStream]", logs)
            else:
                # If the logs are in a different format, raise an error
                raise TypeError("Logs should be either a list of strings or a generator of log lines.")
            # Extend LogSourceInfo
            source_list.extend(sources)

        if not remote_logs:
            sources, served_logs = self._read_from_logs_server(assetWatcher, worker_log_rel_path)
            source_list.extend(sources)

        out_stream: LogHandlerOutputStream = _interleave_logs(
            *local_logs,
            *remote_logs,
            *executor_logs,
            *served_logs,
        )

        # Log message source details are grouped: they are not relevant for most users and can
        # distract them from finding the root cause of their errors
        header = [
            StructuredLogMessage(event="::group::Log message source details"),
            *[StructuredLogMessage(event=source) for source in source_list],
            StructuredLogMessage(event="::endgroup::"),
        ]
        end_of_log = False

        with LogStreamAccumulator(out_stream, HEAP_DUMP_SIZE) as stream_accumulator:
            log_pos = stream_accumulator.total_lines
            out_stream = stream_accumulator.stream

            # skip log stream until the last position
            if metadata and "log_pos" in metadata:
                out_stream = islice(out_stream, metadata["log_pos"], None)
            else:
                # first time reading log, add messages before interleaved log stream
                out_stream = chain(header, out_stream)

            return out_stream, {
                "end_of_log": end_of_log,
                "log_pos": log_pos,
            }

        logs = self._read_remote_logs(assetWatcher, metadata)
        return logs

    def read(self, assetWatcher: AssetWatcherModel, metadata: LogMetadata | None = None,):
        """
        Read logs of given task instance from local machine.

        :param task_instance: task instance object
        :param try_number: task instance try_number to read logs from. If None
                            it returns the log of task_instance.try_number
        :param metadata: log metadata, can be used for steaming log reading and auto-tailing.
        :return: a list of listed tuples which order log string by host
        """

        # compatibility for es_task_handler and os_task_handler
        read_result = self._read(assetWatcher, metadata)
        out_stream, metadata = read_result
        # If the out_stream is None or empty, return the read result
        if not out_stream:
            out_stream = cast("Generator[StructuredLogMessage, None, None]", out_stream)
            return out_stream, metadata

        if _is_logs_stream_like(out_stream):
            out_stream = cast("Generator[StructuredLogMessage, None, None]", out_stream)
            return out_stream, metadata
        if isinstance(out_stream, list) and isinstance(out_stream[0], StructuredLogMessage):
            return (log for log in out_stream), metadata
        if isinstance(out_stream, list) and isinstance(out_stream[0], str):
            # If the out_stream is a list of strings, convert it to a generator
            raw_stream = _stream_lines_by_chunk(io.StringIO("".join(out_stream)))
            out_stream = (log for _, _, log in _log_stream_to_parsed_log_stream(raw_stream))
            return out_stream, metadata
        if isinstance(out_stream, str):
            # If the out_stream is a string, convert it to a generator
            raw_stream = _stream_lines_by_chunk(io.StringIO(out_stream))
            out_stream = (log for _, _, log in _log_stream_to_parsed_log_stream(raw_stream))
            return out_stream, metadata
        raise TypeError(
            "Invalid log stream type. Expected a generator of StructuredLogMessage, list of StructuredLogMessage, list of str or str."
            f" Got {type(out_stream).__name__} instead."
            f" Content type: {type(out_stream[0]).__name__ if isinstance(out_stream, (list, tuple)) and out_stream else 'empty'}"
        )

    def _read_remote_logs(self, assetWatcher: int, metadata=None) -> LogResponse | StreamingLogResponse:
        """
        Implement in subclasses to read from the remote service.

        This method should return two lists, messages and logs.

        * Each element in the messages list should be a single message,
          such as, "reading from x file".
        * Each element in the logs list should be the content of one file.
        """
        remote_io = None
        try:
            from airflow.logging_config import get_remote_task_log

            remote_io = get_remote_task_log()
        except Exception:
            pass

        if remote_io is None:
            # Import not found, or explicitly set to None
            raise NotImplementedError

        # This living here is not really a good plan, but it just about works for now.
        # Ideally we move all the read+combine logic in to TaskLogReader and out of the task handler.
        path = self._render_filename(assetWatcher)
        if stream_method := getattr(remote_io, "stream_trigger", None):
            # Use .stream interface if provider's RemoteIO supports it
            sources, logs = stream_method(path, assetWatcher.trigger_id)
            return sources, logs or []
        # Fallback to .read interface
        sources, logs = remote_io.read_trigger(path, assetWatcher.trigger_id)
        return sources, logs or []
