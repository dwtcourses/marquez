/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package marquez.db;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import marquez.common.models.RunState;
import marquez.db.mappers.RunStateRowMapper;
import marquez.db.models.RunStateRow;
import org.jdbi.v3.sqlobject.CreateSqlObject;
import org.jdbi.v3.sqlobject.SqlObject;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.transaction.Transaction;

@RegisterRowMapper(RunStateRowMapper.class)
public interface RunStateDao extends SqlObject {
  @CreateSqlObject
  DatasetDao createDatasetDao();

  @CreateSqlObject
  RunDao createRunDao();

  @Transaction
  default void insert(
      RunStateRow row,
      List<UUID> outputVersionUuids,
      boolean starting,
      boolean done,
      boolean complete) {
    insertRunState(row);
    // State transition
    final Instant updateAt = row.getTransitionedAt();
    createRunDao().updateRunState(row.getRunUuid(), updateAt, row.getState());
    if (starting) {
      createRunDao().updateStartState(row.getRunUuid(), updateAt, row.getUuid());
    }
    if (done) {
      createRunDao().updateEndState(row.getRunUuid(), updateAt, row.getUuid());
    }
    //Todo: This may not be right
    // Modified
    if (complete && outputVersionUuids != null && outputVersionUuids.size() > 0) {
      createDatasetDao().updateLastModifedAt(outputVersionUuids, updateAt);
    }
  }

  @SqlUpdate("INSERT INTO run_states (uuid, transitioned_at, run_uuid, state)"
      + "VALUES (:uuid, :transitionedAt, :runUuid, :state)")
  void insertRunState(@BindBean RunStateRow row);

  @SqlQuery("SELECT * FROM run_states WHERE uuid = :rowUuid")
  Optional<RunStateRow> findBy(UUID rowUuid);

  @SqlQuery("SELECT COUNT(*) FROM run_states")
  int count();

  @SqlQuery(
      "INSERT INTO run_states (uuid, transitioned_at, run_uuid, state)"
          + "VALUES (:uuid, :now, :runUuid, :runStateType) RETURNING *")
  RunStateRow upsert(UUID uuid, Instant now, UUID runUuid, RunState runStateType);
}
