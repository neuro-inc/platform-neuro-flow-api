from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import AsyncIterator, Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import aiohttp
import pytest
from aiohttp.hdrs import AUTHORIZATION
from async_timeout import timeout
from docker import DockerClient
from docker.errors import NotFound as ContainerNotFound
from docker.models.containers import Container
from jose import jwt
from neuro_auth_client import AuthClient, Permission, User
from neuro_auth_client.security import JWT_IDENTITY_CLAIM_OPTIONS
from yarl import URL

from platform_neuro_flow_api.config import PlatformAuthConfig
from platform_neuro_flow_api.storage.base import Project
from tests.integration.conftest import random_name

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def auth_image() -> str:
    with Path("AUTH_SERVER_IMAGE_NAME").open() as f:
        return f.read().strip()


@pytest.fixture(scope="session")
def auth_name() -> str:
    return "platform-neuro-flow-auth"


@pytest.fixture(scope="session")
def auth_jwt_secret() -> str:
    return os.environ.get("NP_JWT_SECRET", "secret")


def _create_url(container: Container, in_docker: bool) -> URL:
    exposed_port = 8080
    if in_docker:
        host, port = container.attrs["NetworkSettings"]["IPAddress"], exposed_port
    else:
        host, port = "0.0.0.0", container.ports[f"{exposed_port}/tcp"][0]["HostPort"]
    return URL(f"http://{host}:{port}")


@pytest.fixture(scope="session")
def _auth_url() -> URL:
    return URL(os.environ.get("AUTH_URL", ""))


@pytest.fixture(scope="session")
def _auth_server(
    docker_client: DockerClient,
    in_docker: bool,
    reuse_docker: bool,
    auth_image: str,
    auth_name: str,
    auth_jwt_secret: str,
    _auth_url: URL,
) -> Iterator[URL]:
    if _auth_url:
        yield _auth_url
        return

    try:
        container = docker_client.containers.get(auth_name)
        if reuse_docker:
            yield _create_url(container, in_docker)
            return
        else:
            container.remove(force=True)
    except ContainerNotFound:
        pass

    # `run` performs implicit `pull`
    container = docker_client.containers.run(
        image=auth_image,
        name=auth_name,
        publish_all_ports=True,
        stdout=False,
        stderr=False,
        detach=True,
        environment={"NP_JWT_SECRET": auth_jwt_secret},
    )
    container.reload()

    yield _create_url(container, in_docker)

    if not reuse_docker:
        container.remove(force=True)


async def wait_for_auth_server(
    url: URL, timeout_s: float = 300, interval_s: float = 1
) -> None:
    last_exc = None
    try:
        async with timeout(timeout_s):
            while True:
                try:
                    async with AuthClient(url=url, token="") as auth_client:
                        await auth_client.ping()
                        break
                except (AssertionError, OSError, aiohttp.ClientError) as exc:
                    last_exc = exc
                logger.debug("waiting for %s: %s", url, last_exc)
                await asyncio.sleep(interval_s)
    except asyncio.TimeoutError:
        pytest.fail(f"failed to connect to {url}: {last_exc}")


@pytest.fixture
async def auth_server(_auth_server: URL) -> AsyncIterator[URL]:
    await wait_for_auth_server(_auth_server)
    return _auth_server


@pytest.fixture
def token_factory(auth_jwt_secret: str) -> Callable[[str], str]:
    def _factory(identity: str) -> str:
        payload = dict.fromkeys(JWT_IDENTITY_CLAIM_OPTIONS, identity)
        return jwt.encode(payload, auth_jwt_secret, algorithm="HS256")

    return _factory


@pytest.fixture
def admin_token(token_factory: Callable[[str], str]) -> str:
    return token_factory("admin")


@pytest.fixture
def cluster_token(token_factory: Callable[[str], str]) -> str:
    return token_factory("cluster")


@pytest.fixture
def no_claim_token(auth_jwt_secret: str) -> str:
    payload: dict[str, Any] = {}
    return jwt.encode(payload, auth_jwt_secret, algorithm="HS256")


@pytest.fixture
async def auth_client(auth_server: URL, admin_token: str) -> AsyncIterator[AuthClient]:
    async with AuthClient(url=auth_server, token=admin_token) as client:
        yield client


@pytest.fixture
def auth_config(auth_server: URL, admin_token: str) -> PlatformAuthConfig:
    return PlatformAuthConfig(url=auth_server, token=admin_token)


@dataclass(frozen=True)
class _User(User):
    name: str = ""
    token: str = ""

    @property
    def headers(self) -> dict[str, str]:
        return {AUTHORIZATION: f"Bearer {self.token}"}


class UserFactory(Protocol):
    async def __call__(
        self,
        name: str | None = None,
        skip_grant: bool = False,
        org_name: str | None = None,
        org_level: bool = False,
        project_name: str | None = None,
    ) -> _User:
        pass


@pytest.fixture
async def regular_user_factory(
    auth_client: AuthClient,
    token_factory: Callable[[str], str],
    admin_token: str,
    cluster_name: str,
) -> AsyncIterator[UserFactory]:
    async def _factory(
        name: str | None = None,
        skip_grant: bool = False,
        org_name: str | None = None,
        org_level: bool = False,
        project_name: str | None = None,
    ) -> _User:
        if not name:
            name = f"user-{random_name()}"

        user = User(name=name)
        await auth_client.add_user(user, token=admin_token)
        if not skip_grant:
            org_path = f"/{org_name}" if org_name else ""
            project_path = f"/{project_name}" if project_name else ""
            name_path = "" if org_level else f"/{name}"
            permissions = [
                Permission(uri=f"flow://{cluster_name}/{name}", action="write")
            ]
            if org_path:
                permissions.append(
                    Permission(
                        uri=f"flow://{cluster_name}{org_path}{name_path}",
                        action="write",
                    )
                )
            if project_path:
                permissions.append(
                    Permission(
                        uri=f"flow://{cluster_name}{org_path}{project_path}",
                        action="write",
                    )
                )
            await auth_client.grant_user_permissions(
                name, permissions, token=admin_token
            )
        return _User(name=user.name, token=token_factory(user.name))

    return _factory


class ProjectGranter(Protocol):
    async def __call__(
        self,
        user: User,
        project: Project,
        *,
        write: bool = False,
        by_name: bool = False,
    ) -> None:
        pass


@pytest.fixture
async def grant_project_permission(
    auth_client: AuthClient,
    token_factory: Callable[[str], str],
    admin_token: str,
    cluster_name: str,
) -> AsyncIterator[ProjectGranter]:
    async def _grant(
        user: User, project: Project, *, write: bool = False, by_name: bool = False
    ) -> None:
        uri = f"flow://{project.cluster}"
        if project.org_name:
            uri += f"/{project.org_name}"
        uri += f"/{project.project_name}"
        if by_name:
            uri += f"/{project.name}"
        else:
            uri += f"/{project.id}"

        permission = Permission(uri=uri, action="write" if write else "read")
        await auth_client.grant_user_permissions(user.name, [permission], admin_token)

    return _grant


@pytest.fixture
async def regular_user(project_name: str, regular_user_factory: UserFactory) -> _User:
    return await regular_user_factory(project_name=project_name)


@pytest.fixture
async def regular_org_user(
    project_name: str, org_name: str, regular_user_factory: UserFactory
) -> _User:
    return await regular_user_factory(project_name=project_name, org_name=org_name)
