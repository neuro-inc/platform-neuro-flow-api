from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime
from decimal import Decimal

from apolo_api_client import (
    Container,
    Job,
    JobRestartPolicy,
    JobStatus,
    JobStatusHistory,
    Resources,
)
from yarl import URL


def make_job(
    job_id: str,
    *,
    status: JobStatus = JobStatus.PENDING,
    tags: Iterable[str] = (),
    description: str = "",
    scheduler_enabled: bool = False,
    created_at: datetime = datetime.now(UTC),
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
    exit_code: int | None = None,
    restart_policy: JobRestartPolicy = JobRestartPolicy.NEVER,
    life_span: float = 3600,
    name: str | None = None,
    container: Container | None = None,
    pass_config: bool = False,
) -> Job:
    if container is None:
        container = Container(image="ubuntu", resources=Resources(100, 0.1))

    return Job(
        id=job_id,
        owner="test-user",
        cluster_name="default",
        org_name="test-org",
        project_name="test-project",
        status=status,
        history=JobStatusHistory(
            status=status,
            reason="",
            description="",
            restarts=0,
            created_at=created_at,
            started_at=started_at,
            finished_at=finished_at,
            exit_code=exit_code,
        ),
        container=container,
        scheduler_enabled=scheduler_enabled,
        uri=URL(f"job://default/test-user/{job_id}"),
        total_price_credits=Decimal("100"),
        price_credits_per_hour=Decimal("1"),
        name=name,
        tags=sorted(set(tags) | {"project:test", "flow:batch-seq", f"task:{job_id}"}),
        description=description,
        restart_policy=restart_policy,
        life_span=life_span,
        pass_config=pass_config,
    )
