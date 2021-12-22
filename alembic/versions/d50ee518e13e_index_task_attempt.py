"""index_task_attempt

Revision ID: d50ee518e13e
Revises: 18e02ee52b12
Create Date: 2021-05-07 10:53:52.764474

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "d50ee518e13e"
down_revision = "18e02ee52b12"
branch_labels = None
depends_on = None


def upgrade():
    op.create_index(
        "tasks_attempt_id",
        "tasks",
        ["attempt_id"],
    )


def downgrade():
    op.drop_index("tasks_attempt_id")
