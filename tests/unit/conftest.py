from __future__ import annotations

import pytest

from platform_neuro_flow_api.storage.in_memory import InMemoryStorage


@pytest.fixture
def in_memory_storage() -> InMemoryStorage:
    return InMemoryStorage()
