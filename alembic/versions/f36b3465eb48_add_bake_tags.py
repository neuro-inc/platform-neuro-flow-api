"""add bake tags


Revision ID: f36b3465eb48
Revises: c56d0a4eaed5
Create Date: 2021-04-28 18:46:19.425340

"""

import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sapg

from alembic import op

# revision identifiers, used by Alembic.
revision = "f36b3465eb48"
down_revision = "c56d0a4eaed5"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "bakes",
        sa.Column("tags", sapg.JSONB(), nullable=True),
    )
    op.execute(
        "CREATE INDEX bakes_tags_index ON bakes USING GIN (tags jsonb_path_ops);"
    )


def downgrade():
    op.drop_index("bakes_tags_index", table_name="bakes")
    op.drop_column("bakes", "tags")
