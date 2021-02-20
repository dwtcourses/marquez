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

import static marquez.db.Columns.ENDED_AT;
import static marquez.db.Columns.STARTED_AT;
import static marquez.db.OpenLineageDao.DEFAULT_NAMESPACE_OWNER;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import lombok.NonNull;
import marquez.common.Utils;
import marquez.common.models.DatasetId;
import marquez.common.models.JobName;
import marquez.common.models.NamespaceName;
import marquez.common.models.RunState;
import marquez.db.mappers.ExtendedRunRowMapper;
import marquez.db.models.ExtendedDatasetRow;
import marquez.db.models.ExtendedRunRow;
import marquez.db.models.JobRow;
import marquez.db.models.NamespaceRow;
import marquez.db.models.RunArgsRow;
import marquez.db.models.RunRow;
import marquez.service.models.JobMeta;
import marquez.service.models.RunMeta;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.transaction.Transaction;

@RegisterRowMapper(ExtendedRunRowMapper.class)
public interface RunDao extends MarquezDao {
  @Transaction
  default void insert(RunRow row) {
    insertRun(row);
    //todo insert

    // Input versions
    for (UUID inputVersionUuid : row.getInputVersionUuids()) {
      updateInputVersions(row.getUuid(), inputVersionUuid);
    }
  }
  @SqlUpdate("INSERT INTO runs ("
      + "uuid, "
      + "created_at, "
      + "updated_at, "
      + "job_version_uuid, "
      + "run_args_uuid, "
      + "nominal_start_time, "
      + "nominal_end_time"
      + ") VALUES ("
      + ":uuid, "
      + ":createdAt, "
      + ":updatedAt, "
      + ":jobVersionUuid, "
      + ":runArgsUuid, "
      + ":nominalStartTime, "
      + ":nominalEndTime)")
  void insertRun(@BindBean RunRow row);

  @SqlQuery("SELECT EXISTS (SELECT 1 FROM runs WHERE uuid = :rowUuid)")
  boolean exists(UUID rowUuid);

  @SqlUpdate(
      "INSERT INTO runs_input_mapping (run_uuid, dataset_version_uuid) "
          + "VALUES (:runUuid, :datasetVersionUuid) ON CONFLICT DO NOTHING")
  void updateInputVersions(UUID runUuid, UUID datasetVersionUuid);

  @SqlUpdate(
      "UPDATE runs "
          + "SET updated_at = :updatedAt, "
          + "    current_run_state = :currentRunState "
          + "WHERE uuid = :rowUuid")
  void updateRunState(UUID rowUuid, Instant updatedAt, String currentRunState);

  @SqlUpdate(
      "UPDATE runs "
          + "SET updated_at = :updatedAt, "
          + "    start_run_state_uuid = :startRunStateUuid "
          + "WHERE uuid = :rowUuid")
  void updateStartState(UUID rowUuid, Instant updatedAt, UUID startRunStateUuid);

  @SqlUpdate(
      "UPDATE runs "
          + "SET updated_at = :updatedAt, "
          + "    start_run_state_uuid = :startRunStateUuid "
          + "WHERE uuid = :rowUuid AND (updated_at < :updatedAt or start_run_state_uuid is null)")
  void upsertStartState(UUID rowUuid, Instant updatedAt, UUID startRunStateUuid);

  @SqlUpdate(
      "UPDATE runs "
          + "SET updated_at = :updatedAt, "
          + "    end_run_state_uuid = :endRunStateUuid "
          + "WHERE uuid = :rowUuid")
  void updateEndState(UUID rowUuid, Instant updatedAt, UUID endRunStateUuid);

  static final String SELECT_RUN =
      "SELECT r.*, ra.args, rs_s.transitioned_at as "
          + STARTED_AT
          + ", rs_e.transitioned_at as "
          + ENDED_AT
          + ", "
          + "ARRAY(SELECT dataset_version_uuid "
          + "      FROM runs_input_mapping "
          + "      WHERE run_uuid = r.uuid) AS input_version_uuids "
          + "FROM runs AS r "
          + "INNER JOIN run_args AS ra"
          + "  ON (ra.uuid = r.run_args_uuid) "
          + "LEFT JOIN run_states AS rs_s"
          + "  ON (rs_s.uuid = r.start_run_state_uuid) "
          + "LEFT JOIN run_states AS rs_e"
          + "  ON (rs_e.uuid = r.end_run_state_uuid) ";

  @SqlQuery(SELECT_RUN + " WHERE r.uuid = :rowUuid")
  Optional<ExtendedRunRow> findBy(UUID rowUuid);

  @SqlQuery(
      SELECT_RUN
          + "INNER JOIN job_versions AS jv ON r.job_version_uuid = jv.uuid "
          + "WHERE jv.namespace_name = :namespace and jv.job_name = :jobName "
          + "ORDER BY r.created_at DESC "
          + "LIMIT :limit OFFSET :offset")
  List<ExtendedRunRow> findAll(String namespace, String jobName, int limit, int offset);

  @SqlQuery("SELECT COUNT(*) FROM runs")
  int count();

  @SqlQuery(
      "INSERT INTO runs ( "
          + "uuid, "
          + "external_id, "
          + "created_at, "
          + "updated_at, "
          + "job_version_uuid, "
          + "run_args_uuid, "
          + "nominal_start_time, "
          + "nominal_end_time,"
          + "current_run_state, "
          + "current_run_state_time, "
          + "namespace_name, "
          + "job_name "
          + ") VALUES ( "
          + ":runUuid, "
          + ":externalId, "
          + ":now, "
          + ":now, "
          + ":jobVersionUuid, "
          + ":runArgsUuid, "
          + ":nominalStartTime, "
          + ":nominalEndTime, "
          + ":runStateType,"
          + ":runStateTime, "
          + ":namespaceName, "
          + ":jobName "
          + ") ON CONFLICT(uuid) DO "
          + "UPDATE SET "
          + "updated_at = EXCLUDED.updated_at, "
          + "current_run_state = EXCLUDED.current_run_state, "
          + "nominal_start_time = EXCLUDED.nominal_start_time, "
          + "nominal_end_time = EXCLUDED.nominal_end_time "
          + "RETURNING *")
  ExtendedRunRow upsertFromRun(
      UUID runUuid,
      String externalId,
      Instant now,
      UUID jobVersionUuid,
      UUID runArgsUuid,
      Instant nominalStartTime,
      Instant nominalEndTime,
      RunState runStateType,
      Instant runStateTime,
      String namespaceName,
      String jobName);

  @SqlQuery(
      "INSERT INTO runs ( "
          + "uuid, "
          + "external_id, "
          + "created_at, "
          + "updated_at, "
          + "job_version_uuid, "
          + "run_args_uuid, "
          + "nominal_start_time, "
          + "nominal_end_time, "
          + "namespace_name, "
          + "job_name "
          + ") VALUES ( "
          + ":runUuid, "
          + ":externalId, "
          + ":now, "
          + ":now, "
          + ":jobVersionUuid, "
          + ":runArgsUuid, "
          + ":nominalStartTime, "
          + ":nominalEndTime, "
          + ":namespaceName, "
          + ":jobName "
          + ") ON CONFLICT(uuid) DO "
          + "UPDATE SET "
          + "updated_at = EXCLUDED.updated_at, "
          + "current_run_state = EXCLUDED.current_run_state, "
          + "nominal_start_time = EXCLUDED.nominal_start_time "
          + "RETURNING *")
  ExtendedRunRow upsertFromRun(
      UUID runUuid,
      String externalId,
      Instant now,
      UUID jobVersionUuid,
      UUID runArgsUuid,
      Instant nominalStartTime,
      Instant nominalEndTime,
      UUID namespaceUuid,
      String namespaceName,
      String jobName);

  @SqlUpdate(
      "INSERT INTO runs_input_mapping (run_uuid, dataset_version_uuid) "
          + "VALUES (:runUuid, :datasetVersionUuid) ON CONFLICT DO NOTHING")
  void updateInputMapping(UUID runUuid, UUID datasetVersionUuid);

  @Transaction
  default void notifyJobChange(UUID runUuid, JobRow jobRow, JobMeta jobMeta) {
    DatasetDao datasetDao = createDatasetDao();

    upsertRun(runUuid, jobRow.getUuid(), jobRow.getName(), jobRow.getNamespaceName());

    if (jobMeta.getInputs() != null) {
      for (DatasetId datasetId : jobMeta.getInputs()) {
        Optional<ExtendedDatasetRow> datasetRow =
            datasetDao.find(datasetId.getNamespace().getValue(), datasetId.getName().getValue());
        if (datasetRow.isPresent() && datasetRow.get().getCurrentVersionUuid().isPresent()) {
          updateInputMapping(runUuid, datasetRow.get().getCurrentVersionUuid().get());
        }
      }
    }
  }

  @SqlUpdate("UPDATE runs SET job_uuid = :jobUuid, job_name = :jobName, "
      + "namespace_name = :namespaceName"
      + "WHERE uuid = :runUuid")
  void upsertRun(UUID runUuid, @NonNull UUID jobUuid, @NonNull String jobName,
      @NonNull String namespaceName);

  /**
   * Insert from run creates a run but does not associate any datasets.
   */
  @Transaction
  default RunRow upsertFromRun(NamespaceName namespaceName, JobName jobName, RunMeta runMeta,
      RunState currentState) {
    Instant now = Instant.now();
    //Always upsert namespace
    NamespaceRow namespaceRow = createNamespaceDao()
        .upsert(UUID.randomUUID(), now, namespaceName.getValue(), DEFAULT_NAMESPACE_OWNER);

    RunArgsRow runArgsRow = createRunArgsDao()
        .upsert(UUID.randomUUID(), now, Utils.toJson(runMeta.getArgs()), Utils.checksumFor(runMeta.getArgs()));

    UUID uuid = UUID.randomUUID();
    return upsertFromRun(uuid, null, now, null, runArgsRow.getUuid(), runMeta.getNominalStartTime().orElse(null),
        runMeta.getNominalEndTime().orElse(null), currentState, now,
        namespaceRow.getName(), jobName.getValue());
  }

  @SqlUpdate("UPDATE runs SET current_run_state = :runState, current_run_state_time = :transitionedAt WHERE uuid = :runUuid")
  void addRunState(UUID runUuid, RunState runState, Instant transitionedAt);
}
