"""enable cascade removal

Revision ID: dd6533a83f29
Revises: f2f6c4cf6930
Create Date: 2021-09-15 17:50:21.723475

"""
from typing import Any, Optional

from alembic import op


# revision identifiers, used by Alembic.
revision = "dd6533a83f29"
down_revision = "f2f6c4cf6930"
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
    ("bakes", "project_id"),
    ("attempts", "bake_id"),
    ("tasks", "attempt_id"),
    ("cache_entries", "project_id"),
    ("config_files", "bake_id"),
    ("bake_images", "bake_id"),
]


def upgrade():
    for args in FKEYS_AFFECTED:
        _recreate_fkey(*args, ondelete="CASCADE")


def downgrade():
    for args in FKEYS_AFFECTED:
        _recreate_fkey(*args)
