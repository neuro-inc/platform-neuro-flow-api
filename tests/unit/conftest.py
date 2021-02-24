import pytest

from platform_neuro_flow_api.storage import InMemoryStorage


@pytest.fixture
def in_memory_storage() -> InMemoryStorage:
    return InMemoryStorage()
