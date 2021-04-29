"""add bake name

Revision ID: 18e02ee52b12
Revises: f36b3465eb48
Create Date: 2021-04-29 17:50:30.689500

"""
import sqlalchemy as sa

from alembic import op


# revision identifiers, used by Alembic.
revision = "18e02ee52b12"
down_revision = "f36b3465eb48"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "bakes",
        sa.Column("name", sa.String(), nullable=True),
    )
    op.add_column(
        "bakes",
        sa.Column("status", sa.String(), nullable=False, server_default="pending"),
    )
    op.execute("UPDATE bakes SET status = 'succeeded'")
    op.create_index(
        "bakes_name_index",
        "bakes",
        ["name"],
    )
    # Index to simulate conditional unique constraint
    op.create_index(
        "bake_name_project_uq",
        "bakes",
        ["name", "project_id"],
        unique=True,
        postgresql_where=sa.text(
            "(bakes.status != 'succeeded' AND bakes.status != 'failed' AND bakes.status != 'cancelled')"  # noqa
        ),
    )


def downgrade():
    op.drop_index("bake_name_project_uq", table_name="bakes")
    op.drop_index("bakes_name_index", table_name="bakes")
    op.drop_column("bakes", "status")
    op.drop_column("bakes", "name")
