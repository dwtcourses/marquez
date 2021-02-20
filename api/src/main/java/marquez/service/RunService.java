package marquez.service;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkArgument;
import static marquez.common.Utils.KV_JOINER;
import static marquez.common.Utils.VERSION_JOINER;
import static marquez.common.models.RunState.COMPLETED;
import static marquez.common.models.RunState.NEW;

import com.google.common.collect.ImmutableList;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import marquez.api.exceptions.RunNotFoundException;
import marquez.common.models.DatasetName;
import marquez.common.models.DatasetVersionId;
import marquez.common.models.JobName;
import marquez.common.models.JobVersionId;
import marquez.common.models.NamespaceName;
import marquez.common.models.RunId;
import marquez.common.models.RunState;
import marquez.db.DatasetDao;
import marquez.db.DatasetVersionDao;
import marquez.db.JobDao;
import marquez.db.JobVersionDao;
import marquez.db.JobVersionDao.JobVersionBag;
import marquez.db.MarquezDao;
import marquez.db.RunArgsDao;
import marquez.db.RunDao;
import marquez.db.RunStateDao;
import marquez.db.models.ExtendedDatasetVersionRow;
import marquez.db.models.ExtendedRunRow;
import marquez.db.models.JobVersionRow;
import marquez.db.models.RunRow;
import marquez.service.RunTransitionListener.JobInputUpdate;
import marquez.service.RunTransitionListener.JobOutputUpdate;
import marquez.service.RunTransitionListener.RunInput;
import marquez.service.RunTransitionListener.RunOutput;
import marquez.service.RunTransitionListener.RunTransition;
import marquez.service.exceptions.MarquezServiceException;
import marquez.service.mappers.Mapper;
import marquez.service.models.LineageEvent;
import marquez.service.models.Run;
import marquez.service.models.RunMeta;

@Slf4j
public class RunService {
  private final JobVersionDao jobVersionDao;
  private final DatasetDao datasetDao;
  private final RunArgsDao runArgsDao;
  private final RunDao runDao;
  private final DatasetVersionDao datasetVersionDao;
  private final RunStateDao runStateDao;
  private final JobDao jobDao;
  private final Collection<RunTransitionListener> runTransitionListeners;

  public RunService(
      @NonNull MarquezDao marquezDao, Collection<RunTransitionListener> runTransitionListeners) {
    this.jobVersionDao = marquezDao.createJobVersionDao();
    this.datasetDao = marquezDao.createDatasetDao();
    this.runArgsDao = marquezDao.createRunArgsDao();
    this.runDao = marquezDao.createRunDao();
    this.datasetVersionDao = marquezDao.createDatasetVersionDao();
    this.runStateDao = marquezDao.createRunStateDao();
    this.jobDao = marquezDao.createJobDao();
    this.runTransitionListeners = runTransitionListeners;
  }

  public RunService(
      JobVersionDao jobVersionDao,
      DatasetDao datasetDao,
      RunArgsDao runArgsDao,
      RunDao runDao,
      DatasetVersionDao datasetVersionDao,
      RunStateDao runStateDao,
      JobDao jobDao,
      Collection<RunTransitionListener> runTransitionListeners) {
    this.jobVersionDao = jobVersionDao;
    this.datasetDao = datasetDao;
    this.runArgsDao = runArgsDao;
    this.runDao = runDao;
    this.datasetVersionDao = datasetVersionDao;
    this.runStateDao = runStateDao;
    this.jobDao = jobDao;
    this.runTransitionListeners = runTransitionListeners;
  }

  public Run createRun(
      @NonNull NamespaceName namespaceName, @NonNull JobName jobName, @NonNull RunMeta runMeta) {
    log.info("Creating run for job '{}'...", jobName.getValue());
    RunRow runRow = runDao.upsertFromRun(namespaceName, jobName, runMeta, NEW);
    return getRun(RunId.of(runRow.getUuid())).get();
  }

  public boolean runExists(@NonNull RunId runId) throws MarquezServiceException {
    return runDao.exists(runId.getValue());
  }

  public ExtendedRunRow getRun(UUID runId) {
    return runDao.findBy(runId).orElseThrow(() -> new RunNotFoundException(RunId.of(runId)));
  }

  public Optional<Run> getRun(RunId runId) throws MarquezServiceException {
    return runDao.findBy(runId.getValue()).map(Mapper::toRun);
  }

  public ImmutableList<Run> getAllRunsFor(
      @NonNull NamespaceName namespaceName, @NonNull JobName jobName, int limit, int offset)
      throws MarquezServiceException {
    checkArgument(limit >= 0, "limit must be >= 0");
    checkArgument(offset >= 0, "offset must be >= 0");

    final List<ExtendedRunRow> runRows =
        runDao.findAll(namespaceName.getValue(), jobName.getValue(), limit, offset);
    final List<Run> runs = Mapper.toRuns(runRows);
    return ImmutableList.copyOf(runs);
  }

  public void markRunAs(
      @NonNull RunId runId, @NonNull RunState runState, @Nullable Instant transitionedAt) {
    log.debug("Marking run with ID '{}' as '{}'...", runId, runState);
    ExtendedRunRow runRow = runDao.findBy(runId.getValue()).get();
    runDao.addRunState(runId.getValue(), runState, transitionedAt);

    if (runState == COMPLETED) {
      notifyCompleteHandler(runRow, transitionedAt);
    }

    final RunState oldRunState = runRow.getCurrentRunState().map(RunState::valueOf).orElse(null);
    notify(new RunTransition(runId,
        oldRunState,
        runState));
    JobMetrics.emitRunStateCounterMetric(runState);
  }

  public void notifyCompleteHandler(ExtendedRunRow runRow, Instant transitionedAt) {
    JobVersionBag jobVersionBag = jobVersionDao.createJobVersionOnComplete(transitionedAt, runRow.getUuid(), runRow.getNamespaceName(), runRow.getJobName());

    notify(new JobOutputUpdate(RunId.of(runRow.getUuid()), toJobVersionId(jobVersionBag.getJobVersionRow()), buildRunOutputs(jobVersionBag.getOutputs())));
    notify(new JobInputUpdate(RunId.of(runRow.getUuid()), buildRunMeta(null), toJobVersionId(jobVersionBag.getJobVersionRow()), buildRunInputs(jobVersionBag.getInputs())));
  }

  private RunMeta buildRunMeta(ExtendedRunRow runRow) {
    return null;
  }

  private List<RunInput> buildRunInputs(List<ExtendedDatasetVersionRow> inputs) {
    return inputs.stream()
        .map(
            (v) ->
                new RunInput(
                    new DatasetVersionId(
                        NamespaceName.of(v.getNamespaceName()),
                        DatasetName.of(v.getDatasetName()),
                        v.getUuid())))
        .collect(Collectors.toList());
  }

  private List<RunOutput> buildRunOutputs(List<ExtendedDatasetVersionRow> outputs) {
    return outputs.stream()
            .map(
                (v) ->
                    new RunOutput(
                        new DatasetVersionId(
                            NamespaceName.of(v.getNamespaceName()),
                            DatasetName.of(v.getDatasetName()),
                            v.getUuid())))
            .collect(Collectors.toList());
  }

  private JobVersionId toJobVersionId(JobVersionRow jobVersion) {
    return JobVersionId.builder()
            .versionUuid(jobVersion.getUuid())
            .namespace(NamespaceName.of(jobVersion.getNamespaceName()))
            .name(JobName.of(jobVersion.getJobName()))
            .build();
  }

  UUID buildJobVersion(LineageEvent event, Map<String, String> context) {
    final byte[] bytes =
        VERSION_JOINER
            .join(
                event.getJob().getNamespace(),
                event.getJob().getName(),
                event.getProducer(),
                KV_JOINER.join(context))
            .getBytes(UTF_8);
    return UUID.nameUUIDFromBytes(bytes);
  }

  void notify(JobInputUpdate update) {
    notify(RunTransitionListener::notify, update);
  }

  void notify(JobOutputUpdate update) {
    notify(RunTransitionListener::notify, update);
  }

  void notify(RunTransition transition) {
    notify(RunTransitionListener::notify, transition);
  }

  private <T> void notify(BiConsumer<RunTransitionListener, T> f, T param) {
    for (RunTransitionListener runTransitionListener : runTransitionListeners) {
      try {
        f.accept(runTransitionListener, param);
      } catch (Exception e) {
        log.error("Exception from listener " + runTransitionListener, e);
      }
    }
  }
}
