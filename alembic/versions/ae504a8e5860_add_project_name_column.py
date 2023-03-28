"""add project_name column

Revision ID: ae504a8e5860
Revises: 0647beeadb7d
Create Date: 2023-03-22 09:55:16.375384

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "ae504a8e5860"
down_revision = "0647beeadb7d"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("projects", sa.Column("project_name", sa.String()))
    op.execute("UPDATE projects SET project_name = owner")
    op.alter_column("projects", "project_name", nullable=False)
    op.drop_index("projects_name_owner_cluster_org_null_uq", table_name="projects")
    op.drop_index("projects_name_owner_cluster_org_uq", table_name="projects")
    op.create_index(
        "projects_name_project_cluster_org_null_uq",
        "projects",
        ["name", "project_name", "cluster"],
        unique=True,
        postgresql_where=sa.text("org_name IS NULL"),
    )
    op.create_index(
        "projects_name_project_cluster_org_uq",
        "projects",
        ["name", "project_name", "cluster", "org_name"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("projects_name_project_cluster_org_null_uq", table_name="projects")
    op.drop_index("projects_name_project_cluster_org_uq", table_name="projects")
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
    op.drop_column("projects", "project_name")
