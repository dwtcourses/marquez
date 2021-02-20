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

package marquez.service;

import static com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import marquez.common.Utils;
import marquez.common.models.JobName;
import marquez.common.models.JobVersionId;
import marquez.common.models.NamespaceName;
import marquez.db.DatasetDao;
import marquez.db.JobContextDao;
import marquez.db.JobDao;
import marquez.db.JobVersionDao;
import marquez.db.MarquezDao;
import marquez.db.NamespaceDao;
import marquez.db.RunDao;
import marquez.db.models.JobContextRow;
import marquez.db.models.JobRow;
import marquez.service.exceptions.MarquezServiceException;
import marquez.service.mappers.Mapper;
import marquez.service.models.Job;
import marquez.service.models.JobMeta;

@Slf4j
public class JobService {
  private final JobDao jobDao;
  private final RunDao runDao;
  private final ObjectMapper mapper = Utils.newObjectMapper();
  private final JobContextDao jobContextDao;

  public JobService(@NonNull MarquezDao marquezDao, @NonNull final RunService runService) {
    this.jobDao = marquezDao.createJobDao();
    this.runDao = marquezDao.createRunDao();
    this.jobContextDao = marquezDao.createJobContextDao();
  }

  public JobService(
      @NonNull final NamespaceDao namespaceDao,
      @NonNull final DatasetDao datasetDao,
      @NonNull final JobDao jobDao,
      @NonNull final JobVersionDao versionDao,
      @NonNull final JobContextDao contextDao,
      @NonNull final RunDao runDao,
      @NonNull final RunService runService) {
    this.jobDao = jobDao;
    this.runDao = runDao;
    this.jobContextDao = contextDao;
  }

  public Job createOrUpdate(
      @NonNull NamespaceName namespaceName, @NonNull JobName jobName, @NonNull JobMeta jobMeta)
      throws MarquezServiceException {
    JobRow jobRow = jobDao.upsert(namespaceName, jobName, jobMeta, mapper);

    //Run updates come in through this endpoint to notify of input and output datasets.
    // There is an alternative route to registering /output/ datasets in the dataset api.
    if (jobMeta.getRunId().isPresent()) {
      runDao.notifyJobChange(jobMeta.getRunId().get().getValue(), jobRow, jobMeta);
    }

    JobMetrics.emitJobCreationMetric(namespaceName.getValue(), jobMeta.getType().toString());

    return toJob(jobRow);
  }

  public boolean exists(@NonNull NamespaceName namespaceName, @NonNull JobName jobName)
      throws MarquezServiceException {
    return jobDao.exists(namespaceName.getValue(), jobName.getValue());
  }

  public Optional<Job> get(@NonNull NamespaceName namespaceName, @NonNull JobName jobName)
      throws MarquezServiceException {
    return jobDao.find(namespaceName.getValue(), jobName.getValue()).map(this::toJob);
  }

  public Optional<Job> getBy(@NonNull JobVersionId jobVersionId) throws MarquezServiceException {
    return jobDao
        .find(jobVersionId.getNamespace().getValue(), jobVersionId.getName().getValue())
        .map(jobRow -> toJob(jobRow, jobVersionId.getVersionUuid()));
  }

  public ImmutableList<Job> getAll(@NonNull NamespaceName namespaceName, int limit, int offset)
      throws MarquezServiceException {
    checkArgument(limit >= 0, "limit must be >= 0");
    checkArgument(offset >= 0, "offset must be >= 0");
    final ImmutableList.Builder<Job> jobs = ImmutableList.builder();
    final List<JobRow> jobRows = jobDao.findAll(namespaceName.getValue(), limit, offset);
    for (final JobRow jobRow : jobRows) {
      jobs.add(toJob(jobRow));
    }
    return jobs.build();
  }

  /** Creates a {@link Job} instance from the given {@link JobRow}. */
  private Job toJob(@NonNull JobRow jobRow) {
    return toJob(jobRow, null);
  }

  private Job toJob(@NonNull JobRow jobRow, @Nullable UUID jobVersionUuid) {
    Optional<JobContextRow> jobContextRow = jobContextDao.findBy(jobRow.getJobContextUuid());
    //TODO: What run row is this?
    return  Mapper.toJob(
        jobRow,
        jobRow.getInputs(),
        jobRow.getOutputs(),
        jobRow.getLocation(),
        jobContextRow.map(JobContextRow::getContext).orElse("{}"),
        null);
  }
}
