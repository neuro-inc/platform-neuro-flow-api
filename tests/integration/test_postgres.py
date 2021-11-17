import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine


@pytest.mark.asyncio
async def test_postgres_available(sqalchemy_engine: AsyncEngine) -> None:
    async with sqalchemy_engine.connect() as connection:
        result = await connection.execute(sa.text("SELECT 2 + 2;"))
        row = result.one()
        assert row == (4,)
