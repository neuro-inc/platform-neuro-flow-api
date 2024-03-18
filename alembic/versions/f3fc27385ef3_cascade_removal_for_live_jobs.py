"""cascade removal for live jobs

Revision ID: f3fc27385ef3
Revises: dd6533a83f29
Create Date: 2021-09-22 17:49:31.070060

"""

from typing import Any, Optional

from alembic import op

# revision identifiers, used by Alembic.
revision = "f3fc27385ef3"
down_revision = "dd6533a83f29"
branch_labels = None
depends_on = None


def _recreate_fkey(
    source_table: str,
    column: str,
    target_table: Optional[str] = None,
    target_column: str = "id",
    **kwargs: Any,
) -> None:
    target_table = column.replace("_id", "s")
    constraint_name = f"{source_table}_{column}_fkey"
    op.drop_constraint(constraint_name, source_table, "foreignkey")
    op.create_foreign_key(
        constraint_name, source_table, target_table, [column], [target_column], **kwargs
    )


FKEYS_AFFECTED = [
    ("live_jobs", "project_id"),
]


def upgrade():
    for args in FKEYS_AFFECTED:
        _recreate_fkey(*args, ondelete="CASCADE")


def downgrade():
    for args in FKEYS_AFFECTED:
        _recreate_fkey(*args)
