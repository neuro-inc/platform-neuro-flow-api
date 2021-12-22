"""add bake images

Revision ID: f2f6c4cf6930
Revises: d50ee518e13e
Create Date: 2021-05-17 12:19:07.538465

"""
import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sapg

from alembic import op

# revision identifiers, used by Alembic.
revision = "f2f6c4cf6930"
down_revision = "d50ee518e13e"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "bake_images",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("bake_id", sa.String(), sa.ForeignKey("bakes.id"), nullable=False),
        sa.Column("ref", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("payload", sapg.JSONB(), nullable=False),
    )
    op.create_index(
        "bake_images_ref_uq",
        "bake_images",
        ["bake_id", "ref"],
        unique=True,
    )


def downgrade():
    op.drop_table("bake_images")
