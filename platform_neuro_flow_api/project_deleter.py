import logging
from dataclasses import replace
from typing import Self

from apolo_events_client import (
    EventsClientConfig,
    EventType,
    RecvEvent,
    StreamType,
    from_config,
)
from apolo_kube_client import KubeClient
from apolo_kube_client.apolo import generate_namespace_name

from platform_neuro_flow_api.storage.base import Task, TaskStatus

from .storage.base import Storage

logger = logging.getLogger(__name__)


class ProjectDeleter:
    ADMIN_STREAM = StreamType("platform-admin")
    PROJECT_REMOVE = EventType("project-remove")

    def __init__(
        self,
        storage: Storage,
        kube: KubeClient,
        config_events: EventsClientConfig | None = None,
    ) -> None:
        self._storage = storage
        self._client = from_config(config_events)
        self._kube = kube

    async def __aenter__(self) -> Self:
        logger.info("Subscribe for %r", self.ADMIN_STREAM)
        await self._client.subscribe_group(
            self.ADMIN_STREAM, self._on_admin_event, auto_ack=True
        )
        logger.info("Subscribed")
        return self

    async def __aexit__(self, exc_typ: object, exc_val: object, exc_tb: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _delete_task_k8s_pod(self, task: Task, namespace: str) -> None:
        if task.raw_id:
            try:
                await self._kube.core_v1.pod.delete(
                    name=task.raw_id, namespace=namespace
                )
            except Exception as e:
                logger.warning(
                    "Failed to delete pod %r in namespace %r: %r",
                    task.raw_id,
                    namespace,
                    e,
                )

    async def _on_admin_event(self, ev: RecvEvent) -> None:  # noqa: C901
        if ev.event_type == self.PROJECT_REMOVE:
            assert ev.org
            assert ev.project

            namespace = generate_namespace_name(ev.org, ev.project)

            async for project in self._storage.projects.list(
                project_name=ev.project, org_name=ev.org
            ):
                # Delete all live jobs related to the project
                async for live_job in self._storage.live_jobs.list(
                    project_id=project.id
                ):
                    await self._storage.live_jobs.delete(id=live_job.id)

                # Delete all cache entries related to the project
                async for cache_entry in self._storage.cache_entries.list(
                    project_id=project.id
                ):
                    await self._storage.cache_entries.delete(id=cache_entry.id)

                # Delete all bakes related to the project
                async for bake in self._storage.bakes.list(project_id=project.id):
                    # Delete all attempts related to the bake
                    async for attempt in self._storage.attempts.list(bake_id=bake.id):
                        if attempt.result in (TaskStatus.PENDING, TaskStatus.RUNNING):
                            # Mark the attempt as cancelled
                            new_attempt = replace(attempt, result=TaskStatus.CANCELLED)
                            await self._storage.attempts.update(new_attempt)

                            # Delete all k8s pods related to the attempt
                            async for task in self._storage.tasks.list(
                                attempt_id=attempt.id
                            ):
                                await self._delete_task_k8s_pod(task, namespace)

                        # Delete all tasks related to the attempt
                        async for task in self._storage.tasks.list(
                            attempt_id=attempt.id
                        ):
                            await self._storage.tasks.delete(id=task.id)

                        await self._storage.attempts.delete(id=attempt.id)

                    # Delete all bake images related to the bake
                    async for bake_image in self._storage.bake_images.list(
                        bake_id=bake.id
                    ):
                        await self._storage.bake_images.delete(id=bake_image.id)

                    # Delete all config files related to the bake
                    async for config_file in self._storage.config_files.list(
                        bake_id=bake.id
                    ):
                        await self._storage.config_files.delete(id=config_file.id)

                    await self._storage.bakes.delete(id=bake.id)
                await self._storage.projects.delete(id=project.id)
