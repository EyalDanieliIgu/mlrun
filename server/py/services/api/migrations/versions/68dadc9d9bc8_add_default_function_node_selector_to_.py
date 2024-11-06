# Copyright 2023 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Add default function node selector to project

Revision ID: 68dadc9d9bc8
Revises: 0cae47e3a844
Create Date: 2024-03-18 17:57:10.264336

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "68dadc9d9bc8"
down_revision = "0cae47e3a844"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "projects",
        sa.Column("default_function_node_selector", sa.JSON(), nullable=True),
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("projects", "default_function_node_selector")
    # ### end Alembic commands ###