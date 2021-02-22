import asyncio
import logging
import os
from typing import Any, AsyncIterator, Callable, Dict, Iterator

import aiohttp
import pytest
from async_timeout import timeout
from docker import DockerClient
from docker.errors import NotFound as ContainerNotFound
from docker.models.containers import Container
from jose import jwt
from neuro_auth_client import AuthClient
from neuro_auth_client.security import JWT_IDENTITY_CLAIM_OPTIONS
from yarl import URL

from platform_neuro_flow_api.config import PlatformAuthConfig


logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def auth_image() -> str:
    with open("AUTH_SERVER_IMAGE_NAME") as f:
        return f.read().strip()


@pytest.fixture(scope="session")
def auth_name() -> str:
    return "platform-admin-auth"


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
                logger.debug(f"waiting for {url}: {last_exc}")
                await asyncio.sleep(interval_s)
    except asyncio.TimeoutError:
        pytest.fail(f"failed to connect to {url}: {last_exc}")


@pytest.fixture
async def auth_server(_auth_server: URL) -> AsyncIterator[URL]:
    await wait_for_auth_server(_auth_server)
    yield _auth_server


@pytest.fixture
def token_factory(auth_jwt_secret: str) -> Callable[[str], str]:
    def _factory(identity: str) -> str:
        payload = {claim: identity for claim in JWT_IDENTITY_CLAIM_OPTIONS}
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
    payload: Dict[str, Any] = {}
    return jwt.encode(payload, auth_jwt_secret, algorithm="HS256")


@pytest.fixture
async def auth_client(auth_server: URL, admin_token: str) -> AsyncIterator[AuthClient]:
    async with AuthClient(url=auth_server, token=admin_token) as client:
        yield client


@pytest.fixture
def auth_config(auth_server: URL, admin_token: str) -> PlatformAuthConfig:
    return PlatformAuthConfig(url=auth_server, token=admin_token)
