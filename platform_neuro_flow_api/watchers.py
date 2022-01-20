import abc
import asyncio
import contextlib
import logging
from dataclasses import replace
from typing import Any, Optional

from neuro_logging import new_trace
from neuro_sdk import Client as PlatformClient, JobStatus

from .storage.base import AttemptStorage, TaskStatus
from .utils import auto_close

logger = logging.getLogger(__name__)


class Watcher:
    @abc.abstractmethod
    async def check(self) -> None:
        pass


class ExecutorAliveWatcher(Watcher):
    def __init__(
        self, storage: AttemptStorage, platform_client: PlatformClient
    ) -> None:
        self._storage = storage
        self._platform_client = platform_client

    async def check(self) -> None:
        running_results = {
            TaskStatus.PENDING,
            TaskStatus.RUNNING,
        }
        attempts = self._storage.list(results=running_results)
        async with auto_close(attempts):  # type: ignore[arg-type]
            async for attempt in attempts:
                if attempt.executor_id:
                    try:
                        job = await self._platform_client.jobs.status(
                            attempt.executor_id
                        )
                    except Exception:
                        logger.exception(
                            f"Failed to check status of executor {attempt.executor_id}"
                        )
                    else:
                        if not job.status.is_finished:
                            continue
                        # Re-fetch attempt to avoid race condition:
                        # 1. We fetch running attempt
                        # 2. Executor exits and sets attempt to succeeded
                        # 3. We set attempt to failed
                        attempt = await self._storage.get(attempt.id)
                        if attempt.result not in running_results:
                            continue
                        if job.status == JobStatus.CANCELLED:
                            attempt = replace(attempt, result=TaskStatus.CANCELLED)
                        else:
                            attempt = replace(attempt, result=TaskStatus.FAILED)
                        await self._storage.update(attempt)


class WatchersPoller:
    def __init__(self, watchers: list[Watcher], interval_sec: int = 60) -> None:
        self._loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
        self._watchers = watchers
        self._interval_sec = interval_sec

        self._task: Optional[asyncio.Task[None]] = None

    async def __aenter__(self) -> "WatchersPoller":
        await self.start()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.stop()

    async def start(self) -> None:
        if self._task is not None:
            raise RuntimeError("Concurrent usage of watchers poller not allowed")
        names = ", ".join(self._get_watcher_name(e) for e in self._watchers)
        logger.info(f"Starting watchers polling with [{names}]")
        self._task = self._loop.create_task(self._run())

    async def stop(self) -> None:
        logger.info("Stopping watchers polling")
        assert self._task is not None
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task

    async def _run(self) -> None:
        while True:
            start = self._loop.time()
            await self._run_once()
            elapsed = self._loop.time() - start
            delay = self._interval_sec - elapsed
            if delay < 0:
                delay = 0
            await asyncio.sleep(delay)

    @new_trace
    async def _run_once(self) -> None:
        for watcher in self._watchers:
            try:
                await watcher.check()
            except asyncio.CancelledError:
                raise
            except BaseException:
                name = f"watcher {self._get_watcher_name(watcher)}"
                logger.exception(f"Failed to run iteration of the {name}, ignoring...")

    def _get_watcher_name(self, enforcer: Watcher) -> str:
        return type(enforcer).__name__
