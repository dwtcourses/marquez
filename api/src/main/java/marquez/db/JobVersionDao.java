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

import static com.google.common.base.Charsets.UTF_8;
import static java.util.stream.Collectors.joining;
import static marquez.common.Utils.KV_JOINER;
import static marquez.common.Utils.VERSION_DELIM;
import static marquez.common.Utils.VERSION_JOINER;

import com.fasterxml.jackson.core.type.TypeReference;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import lombok.NonNull;
import lombok.Value;
import marquez.common.Utils;
import marquez.db.mappers.ExtendedJobVersionRowMapper;
import marquez.db.models.ExtendedDatasetVersionRow;
import marquez.db.models.ExtendedJobVersionRow;
import marquez.db.models.JobContextRow;
import marquez.db.models.JobRow;
import marquez.db.models.JobVersionRow;
import marquez.db.models.NamespaceRow;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.transaction.Transaction;

@RegisterRowMapper(ExtendedJobVersionRowMapper.class)
public interface JobVersionDao extends MarquezDao {
  enum IoType {
    INPUT,
    OUTPUT;
  }

  @Transaction
  default void insert(@NonNull JobVersionRow row) {
    insert_job_only(row);

    // I/O
    for (UUID inputUuid : row.getInputUuids()) {
      updateInputsOrOutputs(row.getUuid(), inputUuid, IoType.INPUT.name());
    }
    for (UUID outputUuid : row.getOutputUuids()) {
      updateInputsOrOutputs(row.getUuid(), outputUuid, IoType.OUTPUT.name());
    }

    // Version
    final Instant updatedAt = row.getCreatedAt();
    createJobDao().updateVersion(row.getJobUuid(), updatedAt, row.getUuid());
  }

  @SqlUpdate(
      "INSERT INTO job_versions ("
          + "uuid, "
          + "created_at, "
          + "updated_at, "
          + "job_uuid, "
          + "version, "
          + "location, "
          + "latest_run_uuid, "
          + "job_name, "
          + "namespace_uuid, "
          + "namespace_name, "
          + "job_context_uuid"
          + ") VALUES ("
          + ":uuid, "
          + ":createdAt, "
          + ":updateAt, "
          + ":jobUuid, "
          + ":version, "
          + ":location, "
          + ":latestRunUuid, "
          + ":jobName, "
          + ":namespaceUuid, "
          + ":namespaceName, "
          + ":jobContextUuid)")
  void insert_job_only(@BindBean JobVersionRow row);

  @SqlQuery("SELECT EXISTS (SELECT 1 FROM job_versions WHERE version = :version)")
  boolean exists(UUID version);

  @SqlUpdate(
      "INSERT INTO job_versions_io_mapping (job_version_uuid, dataset_uuid, io_type) "
          + "VALUES (:versionUuid, :datasetUuid, :ioType)")
  void updateInputsOrOutputs(UUID versionUuid, UUID datasetUuid, String ioType);

  @SqlUpdate(
      "UPDATE job_versions "
          + "SET updated_at = :updatedAt, "
          + "    latest_run_uuid = :latestRunUuid "
          + "WHERE uuid = :rowUuid")
  void updateLatestRun(UUID rowUuid, Instant updatedAt, UUID latestRunUuid);

  final String EXTENDED_SELECT =
      "SELECT jv.namespace_uuid, jv.*, jc.uuid AS job_context_uuid, jc.context, jv.namespace_name, jv.job_name as name, "
          + "ARRAY(SELECT dataset_uuid "
          + "      FROM job_versions_io_mapping "
          + "      WHERE job_version_uuid = jv.uuid AND "
          + "            io_type = 'INPUT') AS input_uuids, "
          + "ARRAY(SELECT dataset_uuid "
          + "      FROM job_versions_io_mapping "
          + "      WHERE job_version_uuid = jv.uuid AND "
          + "            io_type = 'OUTPUT') AS output_uuids "
          + "FROM job_versions AS jv "
          + "INNER JOIN job_contexts AS jc "
          + "  ON job_context_uuid = jc.uuid ";

  @SqlQuery(EXTENDED_SELECT + "WHERE jv.uuid = :rowUuid")
  Optional<ExtendedJobVersionRow> findBy(UUID rowUuid);

  @SqlQuery(
      EXTENDED_SELECT
          + "INNER JOIN jobs AS j "
          + "  ON j.uuid = jv.job_uuid "
          + "WHERE jv.namespace_name = :namespaceName AND jv.job_name = :jobName AND j.current_version_uuid = jv.uuid "
          + "ORDER BY created_at DESC "
          + "LIMIT 1")
  Optional<ExtendedJobVersionRow> findLatest(String namespaceName, String jobName);

  @SqlQuery(EXTENDED_SELECT + "WHERE jv.version = :version")
  Optional<ExtendedJobVersionRow> findVersion(UUID version);

  @SqlQuery(
      EXTENDED_SELECT
          + "WHERE jv.namespace_name = :namespaceName AND jv.job_name = :jobName "
          + "ORDER BY created_at DESC "
          + "LIMIT :limit OFFSET :offset")
  List<ExtendedJobVersionRow> findAll(String namespaceName, String jobName, int limit, int offset);

  @SqlQuery("SELECT COUNT(*) FROM job_versions")
  int count();

  @SqlQuery(
      "INSERT INTO job_versions ("
          + "uuid, "
          + "created_at, "
          + "updated_at, "
          + "job_uuid, "
          + "job_context_uuid, "
          + "location,"
          + "version,"
          + "job_name,"
          + "namespace_uuid,"
          + "namespace_name"
          + ") VALUES ("
          + ":uuid, "
          + ":now, "
          + ":now, "
          + ":jobUuid, "
          + ":jobContextUuid, "
          + ":location, "
          + ":version, "
          + ":jobName, "
          + ":namespaceUuid, "
          + ":namespaceName) "
          + "ON CONFLICT(version) DO "
          + "UPDATE SET "
          + "updated_at = EXCLUDED.updated_at, "
          + "job_context_uuid = EXCLUDED.job_context_uuid "
          + "RETURNING *")
  ExtendedJobVersionRow upsert(
      UUID uuid,
      Instant now,
      UUID jobUuid,
      UUID jobContextUuid,
      String location,
      UUID version,
      String jobName,
      UUID namespaceUuid,
      String namespaceName);

  @SqlUpdate(
      "INSERT INTO job_versions_io_mapping ("
          + "job_version_uuid, dataset_uuid, io_type) "
          + "VALUES (:jobVersionUuid, :datasetUuid, :ioType) ON CONFLICT DO NOTHING")
  void upsertDatasetIoMapping(UUID jobVersionUuid, UUID datasetUuid, IoType ioType);

  default JobVersionBag createJobVersionOnComplete(Instant transitionedAt, UUID runUuid, String namespaceName, String jobName) {
    DatasetVersionDao datasetVersionDao = createDatasetVersionDao();
    JobVersionDao jobVersionDao = createJobVersionDao();
    JobDao jobDao = createJobDao();
    JobContextDao jobContextDao = createJobContextDao();
    JobRow jobRow = jobDao.find(namespaceName, jobName).get();
    Optional<JobContextRow> jobContextRow = jobContextDao.findBy(jobRow.getJobContextUuid());
    Map context = jobContextRow.map(e-> Utils.fromJson(e.getContext(), new TypeReference<Map<String, String>>() {})).orElse(new HashMap<>());
    List<ExtendedDatasetVersionRow> inputs = datasetVersionDao.getInputDatasets(runUuid);
    List<ExtendedDatasetVersionRow> outputs = datasetVersionDao.findByRunId(runUuid);
    NamespaceRow namespaceRow = createNamespaceDao().findBy(jobRow.getNamespaceName()).get();

    JobVersionRow jobVersion =
        jobVersionDao.upsert(
            UUID.randomUUID(),
            transitionedAt,
            jobRow.getUuid(),
            jobRow.getJobContextUuid(),
            jobRow.getLocation(),
            buildJobVersion(jobRow.getNamespaceName(), jobRow.getName(), inputs, outputs, context, jobRow.getLocation()),
            jobRow.getName(),
            namespaceRow.getUuid(),
            jobRow.getNamespaceName());

    jobDao.updateVersion(jobRow.getUuid(), transitionedAt, jobVersion.getUuid());
    jobVersionDao.updateLatestRun(jobVersion.getUuid(), transitionedAt, runUuid);
    for (ExtendedDatasetVersionRow datasetVersionRow : inputs) {
      jobVersionDao.upsertDatasetIoMapping(
          jobVersion.getUuid(), datasetVersionRow.getDatasetUuid(), IoType.INPUT);
    }

    for (ExtendedDatasetVersionRow datasetVersionRow : outputs) {
      jobVersionDao.upsertDatasetIoMapping(
          jobVersion.getUuid(), datasetVersionRow.getDatasetUuid(), IoType.OUTPUT);
    }

    return new JobVersionBag(jobRow, inputs, outputs, jobVersion);
  }

  default UUID buildJobVersion(String namespaceName, String jobName, List<ExtendedDatasetVersionRow> inputs, List<ExtendedDatasetVersionRow> outputs, Map context, String location) {
    final byte[] bytes =
        VERSION_JOINER
            .join(
                namespaceName,
                jobName,
                inputs.stream().flatMap(JobVersionDao::idToStream).collect(joining(VERSION_DELIM)),
                outputs.stream().flatMap(JobVersionDao::idToStream).collect(joining(VERSION_DELIM)),
                location,
                KV_JOINER.join(context))
            .getBytes(UTF_8);
    return UUID.nameUUIDFromBytes(bytes);
  }

  public static Stream<String> idToStream(ExtendedDatasetVersionRow dataset) {
    return Stream.of(dataset.getNamespaceName(), dataset.getDatasetName());
  }

  @Value
  class JobVersionBag {
    JobRow jobRow;
    List<ExtendedDatasetVersionRow> inputs;
    List<ExtendedDatasetVersionRow> outputs;
    JobVersionRow jobVersionRow;
  }
}