import time
from collections.abc import AsyncIterator, Iterator

import asyncpg
import pytest
from docker import DockerClient
from docker.errors import NotFound as ContainerNotFound
from docker.models.containers import Container
from sqlalchemy.ext.asyncio import AsyncEngine

from platform_neuro_flow_api.config import PostgresConfig
from platform_neuro_flow_api.config_factory import EnvironConfigFactory
from platform_neuro_flow_api.postgres import MigrationRunner, make_async_engine


@pytest.fixture(scope="session")
def _postgres_dsn(
    docker_client: DockerClient,
    in_docker: bool,
    reuse_docker: bool,
    auth_image: str,
    auth_name: str,
    auth_jwt_secret: str,
) -> Iterator[str]:

    image_name = "postgres:11.3"
    container_name = "postgres"

    try:
        container = docker_client.containers.get(container_name)
        if reuse_docker:
            yield _make_postgres_dsn(container)
            return
        else:
            container.remove(force=True)
    except ContainerNotFound:
        pass

    # `run` performs implicit `pull`
    container = docker_client.containers.run(
        image=image_name,
        name=container_name,
        publish_all_ports=True,
        stdout=False,
        stderr=False,
        detach=True,
    )
    container.reload()

    yield _make_postgres_dsn(container)

    if not reuse_docker:
        container.remove(force=True)


def _make_postgres_dsn(container: Container) -> str:
    exposed_port = 5432
    host, port = "0.0.0.0", container.ports[f"{exposed_port}/tcp"][0]["HostPort"]
    return f"postgresql+asyncpg://postgres@{host}:{port}/postgres"


async def _wait_for_postgres_server(
    postgres_dsn: str, attempts: int = 5, interval_s: float = 1
) -> None:
    if postgres_dsn.startswith("postgresql+asyncpg://"):
        postgres_dsn = (
            "postgresql" + postgres_dsn[len("postgresql+asyncpg") :]  # noqa: E203
        )
    attempt = 0
    while attempt < attempts:
        try:
            attempt = attempt + 1
            conn = await asyncpg.connect(postgres_dsn, timeout=5.0)
            await conn.close()
            return
        except Exception:
            pass
        time.sleep(interval_s)


@pytest.fixture
async def postgres_dsn(_postgres_dsn: str) -> str:
    await _wait_for_postgres_server(_postgres_dsn)
    return _postgres_dsn


@pytest.fixture
async def postgres_config(postgres_dsn: str) -> AsyncIterator[PostgresConfig]:

    db_config = PostgresConfig(
        postgres_dsn=postgres_dsn,
        alembic=EnvironConfigFactory().create_alembic(postgres_dsn),
    )
    migration_runner = MigrationRunner(db_config)
    await migration_runner.upgrade()

    yield db_config

    await migration_runner.downgrade()


@pytest.fixture
async def sqalchemy_engine(
    postgres_config: PostgresConfig,
) -> AsyncIterator[AsyncEngine]:
    engine = make_async_engine(postgres_config)
    try:
        yield engine
    finally:
        await engine.dispose()
