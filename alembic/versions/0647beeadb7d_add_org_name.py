"""add org_name

Revision ID: 0647beeadb7d
Revises: f3fc27385ef3
Create Date: 2022-05-12 19:35:03.484368

"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "0647beeadb7d"
down_revision = "f3fc27385ef3"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "projects",
        sa.Column("org_name", sa.String(), nullable=True),
    )
    op.drop_index("projects_name_owner_cluster_uq", table_name="projects")
    op.create_index(
        "projects_name_owner_cluster_org_null_uq",
        "projects",
        ["name", "owner", "cluster"],
        unique=True,
        postgresql_where=sa.text("org_name IS NULL"),
    )
    op.create_index(
        "projects_name_owner_cluster_org_uq",
        "projects",
        ["name", "owner", "cluster", "org_name"],
        unique=True,
    )


def downgrade():
    op.drop_index("projects_name_owner_cluster_org_uq", table_name="projects")
    op.drop_index("projects_name_owner_cluster_org_null_uq", table_name="projects")
    op.drop_column("projects", "org_name")
