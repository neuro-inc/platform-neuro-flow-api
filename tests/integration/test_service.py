import pytest

from platform_neuro_flow_api.service import Service



class TestService:
    @pytest.fixture
    def service(self) -> Service:
        return Service()

    async def test_created(self, service: Service) -> None:
        assert service
