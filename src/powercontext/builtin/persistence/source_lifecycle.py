# Copyright (c) 2026 OceanBase.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Shared Topic qualification predicates; projections and history stay intact."""

from sqlalchemy import select
from sqlalchemy.sql.elements import ColumnElement

from powercontext.builtin.persistence.tables import SOURCE_LIFECYCLE_TABLE


def lifecycle_topic_eligible(scope_id: str, artifact_id: ColumnElement[str]) -> ColumnElement[bool]:
    table = SOURCE_LIFECYCLE_TABLE
    return (
        ~select(table.c.node_id)
        .where(
            table.c.scope_id == scope_id,
            table.c.kind == "topic",
            table.c.artifact_id == artifact_id,
            table.c.available.is_(False),
        )
        .exists()
    )


def topic_eligible_sql(alias: str) -> str:
    # alias is an internal SQL table/alias constant, never user input.
    return (
        "NOT EXISTS (SELECT 1 FROM pc_source_lifecycle AS lifecycle "  # noqa: S608 - static identifier only
        f"WHERE lifecycle.scope_id = {alias}.scope_id AND lifecycle.kind = 'topic' "
        f"AND lifecycle.artifact_id = {alias}.artifact_id AND lifecycle.available = 0)"
    )
