from dataclasses import dataclass
from typing import Optional

import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sapg
from asyncpg.pool import Pool

from .base import Storage


@dataclass(frozen=True)
class FlowTables:
    tmp_table: sa.Table

    @classmethod
    def create(cls) -> "FlowTables":
        metadata = sa.MetaData()
        tmp_table = sa.Table(
            "tmp",
            metadata,
            sa.Column("id", sa.String(), primary_key=True),
            # All other fields
            sa.Column("payload", sapg.JSONB(), nullable=False),
        )
        return cls(
            tmp_table=tmp_table,
        )


class PostgresStorage(Storage):
    def __init__(self, pool: Pool, tables: Optional[FlowTables] = None) -> None:
        self._pool = pool
        self._tables = tables or FlowTables.create()
