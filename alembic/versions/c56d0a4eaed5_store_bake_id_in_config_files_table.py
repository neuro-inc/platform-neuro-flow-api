"""store bake_id in config files table

Revision ID: c56d0a4eaed5
Revises: f00badb90a87
Create Date: 2021-04-26 08:51:55.571516

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "c56d0a4eaed5"
down_revision = "f00badb90a87"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "config_files",
        sa.Column("bake_id", sa.String(), sa.ForeignKey("bakes.id"), nullable=False),
    )


def downgrade():
    op.drop_column("config_files", "bake_id")
