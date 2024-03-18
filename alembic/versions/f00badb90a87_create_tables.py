"""create tables

Revision ID: f00badb90a87
Revises:
Create Date: 2021-02-24 18:08:13.764531

"""

import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sapg

from alembic import op

# revision identifiers, used by Alembic.
revision = "f00badb90a87"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "projects",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("owner", sa.String(), nullable=False),
        sa.Column("cluster", sa.String(), nullable=False),
        sa.Column("payload", sapg.JSONB(), nullable=False),
    )
    op.create_table(
        "live_jobs",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("yaml_id", sa.String(), nullable=False),
        sa.Column(
            "project_id", sa.String(), sa.ForeignKey("projects.id"), nullable=False
        ),
        sa.Column("tags", sapg.JSONB(), nullable=False),
        sa.Column("payload", sapg.JSONB(), nullable=False),
    )
    op.create_table(
        "bakes",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column(
            "project_id", sa.String(), sa.ForeignKey("projects.id"), nullable=False
        ),
        sa.Column("batch", sa.String(), nullable=False),
        sa.Column(
            "created_at", sapg.TIMESTAMP(timezone=True, precision=6), nullable=False
        ),
        sa.Column("payload", sapg.JSONB(), nullable=False),
    )
    op.create_table(
        "config_files",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("filename", sa.String(), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("payload", sapg.JSONB(), nullable=False),
    )
    op.create_table(
        "attempts",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("bake_id", sa.String(), sa.ForeignKey("bakes.id"), nullable=False),
        sa.Column("number", sa.Integer(), nullable=False),
        sa.Column(
            "created_at", sapg.TIMESTAMP(timezone=True, precision=6), nullable=False
        ),
        sa.Column("result", sa.String(), nullable=False),
        sa.Column("payload", sapg.JSONB(), nullable=False),
    )
    op.create_table(
        "tasks",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column(
            "attempt_id", sa.String(), sa.ForeignKey("attempts.id"), nullable=False
        ),
        sa.Column("yaml_id", sa.String(), nullable=False),
        sa.Column("payload", sapg.JSONB(), nullable=False),
    )
    op.create_table(
        "cache_entries",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column(
            "project_id", sa.String(), sa.ForeignKey("projects.id"), nullable=False
        ),
        sa.Column("batch", sa.String(), nullable=False),
        sa.Column("task_id", sa.String(), nullable=False),
        sa.Column("key", sa.String(), nullable=False),
        sa.Column(
            "created_at", sapg.TIMESTAMP(timezone=True, precision=6), nullable=False
        ),
        sa.Column("payload", sapg.JSONB(), nullable=False),
    )
    # Indexes:
    op.create_index(
        "projects_name_owner_cluster_uq",
        "projects",
        ["name", "owner", "cluster"],
        unique=True,
    )
    op.create_index(
        "live_jobs_project_yaml_id_uq",
        "live_jobs",
        ["project_id", "yaml_id"],
        unique=True,
    )
    op.create_index(
        "bake_project_id_index",
        "bakes",
        ["project_id"],
    )
    op.create_index(
        "attempts_bake_number_uq",
        "attempts",
        ["bake_id", "number"],
        unique=True,
    )
    op.create_index(
        "tasks_attempt_yaml_id_uq",
        "tasks",
        ["attempt_id", "yaml_id"],
        unique=True,
    )
    op.create_index(
        "cache_entries_key_uq",
        "cache_entries",
        ["project_id", "batch", "task_id", "key"],
        unique=True,
    )


def downgrade():
    op.drop_table("cache_entries")
    op.drop_table("tasks")
    op.drop_table("attempts")
    op.drop_table("config_files")
    op.drop_table("bakes")
    op.drop_table("live_jobs")
    op.drop_table("projects")
