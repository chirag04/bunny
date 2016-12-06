package org.rabix.tes.service.impl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.mina.util.ConcurrentHashSet;
import org.rabix.bindings.BindingException;
import org.rabix.bindings.Bindings;
import org.rabix.bindings.BindingsFactory;
import org.rabix.bindings.helper.FileValueHelper;
import org.rabix.bindings.mapper.FileMappingException;
import org.rabix.bindings.mapper.FilePathMapper;
import org.rabix.bindings.model.FileValue;
import org.rabix.bindings.model.FileValue.FileType;
import org.rabix.bindings.model.Job;
import org.rabix.bindings.model.Job.JobStatus;
import org.rabix.bindings.model.requirement.DockerContainerRequirement;
import org.rabix.bindings.model.requirement.Requirement;
import org.rabix.bindings.transformer.FileTransformer;
import org.rabix.common.helper.JSONHelper;
import org.rabix.executor.engine.EngineStub;
import org.rabix.executor.engine.EngineStubActiveMQ;
import org.rabix.executor.engine.EngineStubLocal;
import org.rabix.executor.engine.EngineStubRabbitMQ;
import org.rabix.executor.service.ExecutorService;
import org.rabix.executor.status.ExecutorStatusCallback;
import org.rabix.executor.status.ExecutorStatusCallbackException;
import org.rabix.tes.client.TESHTTPClientException;
import org.rabix.tes.client.TESHttpClient;
import org.rabix.tes.model.TESDockerExecutor;
import org.rabix.tes.model.TESJob;
import org.rabix.tes.model.TESJobId;
import org.rabix.tes.model.TESResources;
import org.rabix.tes.model.TESState;
import org.rabix.tes.model.TESTask;
import org.rabix.tes.model.TESTaskParameter;
import org.rabix.tes.model.TESVolume;
import org.rabix.tes.service.TESServiceException;
import org.rabix.tes.service.TESStorageService;
import org.rabix.tes.service.impl.TESStorageServiceImpl.LocalFileStorage;
import org.rabix.tes.service.impl.TESStorageServiceImpl.SharedFileStorage;
import org.rabix.transport.backend.Backend;
import org.rabix.transport.backend.impl.BackendActiveMQ;
import org.rabix.transport.backend.impl.BackendLocal;
import org.rabix.transport.backend.impl.BackendRabbitMQ;
import org.rabix.transport.mechanism.TransportPluginException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;

public class TESExecutorServiceImpl implements ExecutorService {

  private final static Logger logger = LoggerFactory.getLogger(TESExecutorServiceImpl.class);

  public final static String WORKING_DIR = "working_dir";
  public final static String STANDARD_OUT_LOG = "standard_out.log";
  public final static String STANDARD_ERROR_LOG = "standard_error.log";
  
  public final static String DEFAULT_PROJECT = "default";

  private static final String DEFAULT_COMMAND_LINE_TOOL_ERR_LOG = "job.err.log";
  
  private TESHttpClient tesHttpClient;
  private TESStorageService storageService;

  private final Set<Future<?>> taskFutures = new ConcurrentHashSet<>();
 
  private final Map<String, Job> taskJobs = new HashMap<>();
  
  private final ScheduledExecutorService scheduledTaskChecker = Executors.newScheduledThreadPool(1);
  private final java.util.concurrent.ExecutorService taskPoolExecutor = Executors.newFixedThreadPool(10);
  
  private EngineStub<?, ?, ?> engineStub;
  
  private final Configuration configuration;
  private final ExecutorStatusCallback statusCallback;
  
  @Inject
  public TESExecutorServiceImpl(final TESHttpClient tesHttpClient, final TESStorageService storageService, final ExecutorStatusCallback statusCallback, final Configuration configuration) {
    this.tesHttpClient = tesHttpClient;
    this.storageService = storageService;
    this.configuration = configuration;
    this.statusCallback = statusCallback;
    
    this.scheduledTaskChecker.scheduleAtFixedRate(new Runnable() {
      @Override
      @SuppressWarnings("unchecked")
      public void run() {
        for (Iterator<Future<?>> iterator = taskFutures.iterator(); iterator.hasNext();){
          Future<TESJob> tesJobFuture = (Future<TESJob>) iterator.next();
          if (tesJobFuture.isDone()) {
            try {
              TESJob tesJob = tesJobFuture.get();

              if (tesJob.getState().equals(TESState.Complete)) {
                success(tesJob);
              } else {
                fail(tesJob);
              }
              taskJobs.remove(tesJob.getTask().getTaskId());
              iterator.remove();
            } catch (InterruptedException | ExecutionException e) {
              logger.error("Failed to retrieve TESJob", e); // TODO handle
            }
          }
        }
      }
    }, 0, 1, TimeUnit.SECONDS);
  }
  
  @SuppressWarnings("unchecked")
  private void success(TESJob tesJob) {
    Job job = taskJobs.get(tesJob.getTask().getTaskId());
    job = Job.cloneWithStatus(job, JobStatus.COMPLETED);
    Map<String, Object> result = (Map<String, Object>) FileValue.deserialize(JSONHelper.readMap(tesJob.getLogs().get(tesJob.getLogs().size()-1).getStdout())); // TODO change log fetching
    
    final Job finalJob = job;
    result = (Map<String, Object>) FileValueHelper.updateFileValues(result, new FileTransformer() {
      @Override
      public FileValue transform(FileValue fileValue) {
        String location = fileValue.getLocation();
        if (location.startsWith("/mnt")) {
          location = finalJob.getId() + "/" + location.substring(5);
        }
        fileValue.setPath(location);
        fileValue.setLocation(location);
        return fileValue;
      }
    });
    
    job = Job.cloneWithOutputs(job, result);
    job = Job.cloneWithMessage(job, "Success!");
    try {
      job = statusCallback.onJobCompleted(job);
    } catch (ExecutorStatusCallbackException e1) {
      logger.warn("Failed to execute statusCallback: {}", e1);
    }
    engineStub.send(job);
  }
  
  private void fail(TESJob tesJob) {
    Job job = taskJobs.get(tesJob.getTask().getTaskId());
    job = Job.cloneWithStatus(job, JobStatus.FAILED);
    try {
      job = statusCallback.onJobFailed(job);
    } catch (ExecutorStatusCallbackException e1) {
      logger.warn("Failed to execute statusCallback: {}", e1);
    }
  }
  
  @Override
  public void initialize(Backend backend) {
    try {
      switch (backend.getType()) {
      case LOCAL:
        engineStub = new EngineStubLocal((BackendLocal) backend, this, configuration);
        break;
      case RABBIT_MQ:
        engineStub = new EngineStubRabbitMQ((BackendRabbitMQ) backend, this, configuration);
        break;
      case ACTIVE_MQ:
        engineStub = new EngineStubActiveMQ((BackendActiveMQ) backend, this, configuration);
      default:
        break;
      }
      engineStub.start();
    } catch (TransportPluginException e) {
      logger.error("Failed to initialize Executor", e);
      throw new RuntimeException("Failed to initialize Executor", e);
    }
  }

  @Override
  public void start(Job job, String contextId) {
    taskFutures.add(taskPoolExecutor.submit(new TaskRunCallable(job)));
    taskJobs.put(job.getId(), job);
  }

  @SuppressWarnings("unchecked")
  private <T extends Requirement> T getRequirement(List<Requirement> requirements, Class<T> clazz) {
    for (Requirement requirement : requirements) {
      if (requirement.getClass().equals(clazz)) {
        return (T) requirement;
      }
    }
    return null;
  }
  
  private File createJobDir(SharedFileStorage sharedFileStorage, Job job) {
    File jobDir = new File(sharedFileStorage.getBaseDir(), job.getId());
    if (!jobDir.exists()) {
      jobDir.mkdirs();
    }
    return jobDir;
  }
  
  private File createWorkingDir(SharedFileStorage sharedFileStorage, Job job) {
    File workingDir = new File(sharedFileStorage.getBaseDir(), getWorkingDirRelativePath(job));
    if (!workingDir.exists()) {
      workingDir.mkdirs();
    }
    return workingDir;
  }
  
  private String getWorkingDirRelativePath(Job job) {
    return job.getId() + "/" + WORKING_DIR;
  }
  
  public class TaskRunCallable implements Callable<TESJob> {

    private Job job;
    
    public TaskRunCallable(Job job) {
      this.job = job;
    }
    
    @Override
    public TESJob call() throws Exception {
      try {
        final SharedFileStorage sharedFileStorage = storageService.getStorageInfo();

        LocalFileStorage localFileStorage = new LocalFileStorage(configuration.getString("backend.execution.directory"));
        job = storageService.stageInputFiles(job, localFileStorage, sharedFileStorage);

        List<TESTaskParameter> inputs = new ArrayList<>();
        inputs.add(new TESTaskParameter("mount", null, "", "/mnt", FileType.Directory.name(), true));
        
        job = FileValueHelper.mapInputFilePaths(job, new FilePathMapper() {
          @Override
          public String map(String path, Map<String, Object> config) throws FileMappingException {
            return path.replace(sharedFileStorage.getBaseDir(), "/mnt");
          }
        });
        
        Bindings bindings = BindingsFactory.create(job);
        
        createWorkingDir(sharedFileStorage, job);
        File jobDir = createJobDir(sharedFileStorage, job);

        String workingDirRelativePath = getWorkingDirRelativePath(job);
        inputs.add(new TESTaskParameter("working_dir", null, workingDirRelativePath, "/mnt/working_dir", FileType.Directory.name(), false));

        File jobFile = new File(jobDir, "job.json");
        FileUtils.writeStringToFile(jobFile, JSONHelper.writeObject(job));
        inputs.add(new TESTaskParameter("job.json", null, job.getId() + "/job.json", "/mnt/job.json", FileType.File.name(), true));
        
        List<TESTaskParameter> outputs = new ArrayList<>();
        outputs.add(new TESTaskParameter("working_dir", null, workingDirRelativePath, "/mnt/working_dir", FileType.Directory.name(), false));    
        if (!bindings.canExecute(job)) {
          outputs.add(new TESTaskParameter("command.sh", null, job.getId() + "/command.sh", "/mnt/command.sh", FileType.File.name(), false));
          outputs.add(new TESTaskParameter("environment.sh", null, job.getId() + "/environment.sh", "/mnt/environment.sh", FileType.File.name(), false));
        }
        
        List<String> firstCommandLineParts = new ArrayList<>();
        firstCommandLineParts.add("/usr/share/rabix-tes-command-line/rabix");
        firstCommandLineParts.add("-j");
        firstCommandLineParts.add("/mnt/job.json");
        firstCommandLineParts.add("-w");
        firstCommandLineParts.add("/mnt/working_dir");
        firstCommandLineParts.add("-m");
        firstCommandLineParts.add("initialize");
        
        List<TESDockerExecutor> dockerExecutors = new ArrayList<>();
        String standardOutLog = "/mnt/" + STANDARD_OUT_LOG;
        String standardErrorLog = "/mnt/" + STANDARD_ERROR_LOG;
        
        dockerExecutors.add(new TESDockerExecutor("janko/java-oracle:v6", firstCommandLineParts, "/mnt/working_dir", null, standardOutLog, standardErrorLog));
        
        List<Requirement> combinedRequirements = new ArrayList<>();
        combinedRequirements.addAll(bindings.getHints(job));
        combinedRequirements.addAll(bindings.getRequirements(job));

        DockerContainerRequirement dockerContainerRequirement = getRequirement(combinedRequirements, DockerContainerRequirement.class);
        String imageId = null;
        if (dockerContainerRequirement == null) {
          imageId = "frolvlad/alpine-python2";
        } else {
          if (dockerContainerRequirement.getDockerImageId() != null) {
            imageId = dockerContainerRequirement.getDockerImageId();
          } else {
            imageId = dockerContainerRequirement.getDockerPull();
          }
        }
        
        if (!bindings.canExecute(job)) {
          List<String> secondCommandLineParts = new ArrayList<>();
          secondCommandLineParts.add("/bin/sh");
          secondCommandLineParts.add("../command.sh");

          String commandLineToolStdout = bindings.getStandardOutLog(job);
          if (commandLineToolStdout != null) {
            commandLineToolStdout = "/mnt/working_dir/" + commandLineToolStdout;
          }

          String commandLineToolErrLog = bindings.getStandardErrorLog(job);
          String commandLineStandardErrLog = "/mnt/working_dir/" + (commandLineToolErrLog != null ? commandLineToolErrLog : DEFAULT_COMMAND_LINE_TOOL_ERR_LOG);
          
          dockerExecutors.add(new TESDockerExecutor(imageId, secondCommandLineParts, "/mnt/working_dir", null, commandLineToolStdout, commandLineStandardErrLog));
        }
        
        List<String> thirdCommandLineParts = new ArrayList<>();
        thirdCommandLineParts.add("/usr/share/rabix-tes-command-line/rabix");
        thirdCommandLineParts.add("-j");
        thirdCommandLineParts.add("/mnt/job.json");
        thirdCommandLineParts.add("-w");
        thirdCommandLineParts.add("/mnt/working_dir");
        thirdCommandLineParts.add("-m");
        thirdCommandLineParts.add("finalize");
        
        dockerExecutors.add(new TESDockerExecutor("janko/java-oracle:v6", thirdCommandLineParts, "/mnt/working_dir", null, standardOutLog, standardErrorLog));
        
        List<TESVolume> volumes = new ArrayList<>();
        volumes.add(new TESVolume("vol_work", 1, null, "/mnt"));
        TESResources resources = new TESResources(null, false, null, volumes, null);

        TESTask task = new TESTask(job.getName(), DEFAULT_PROJECT, null, inputs, outputs, resources, job.getId(), dockerExecutors);
        
        TESJobId tesJobId = tesHttpClient.runTask(task);
        taskJobs.put(tesJobId.getValue(), job);
        
        TESJob tesJob = null;
        do {
          Thread.sleep(500L);
          tesJob = tesHttpClient.getJob(tesJobId);
          if (tesJob == null) {
            throw new TESServiceException("TESJob is not created. JobId = " + job.getId());
          }
        } while(!isFinished(tesJob));
        return tesJob;
      } catch (IOException e) {
        logger.error("Failed to write files to SharedFileStorage", e);
        throw new TESServiceException("Failed to write files to SharedFileStorage", e);
      } catch (TESHTTPClientException e) {
        logger.error("Failed to submit Job to TES", e);
        throw new TESServiceException("Failed to submit Job to TES", e);
      } catch (BindingException e) {
        logger.error("Failed to use Bindings", e);
        throw new TESServiceException("Failed to use Bindings", e);
      }
    }
    
    private boolean isFinished(TESJob tesJob) {
      return tesJob.getState().equals(TESState.Canceled) || 
          tesJob.getState().equals(TESState.Complete) || 
          tesJob.getState().equals(TESState.Error) || 
          tesJob.getState().equals(TESState.SystemError);
    }
    
  }
  
  @Override
  public void stop(List<String> ids, String contextId) {
    throw new NotImplementedException("This method is not implemented");
  }

  @Override
  public void free(String rootId, Map<String, Object> config) {
    throw new NotImplementedException("This method is not implemented");
  }

  @Override
  public void shutdown(Boolean stopEverything) {
    throw new NotImplementedException("This method is not implemented");
  }

  @Override
  public boolean isRunning(String id, String contextId) {
    throw new NotImplementedException("This method is not implemented");
  }

  @Override
  public Map<String, Object> getResult(String id, String contextId) {
    throw new NotImplementedException("This method is not implemented");
  }

  @Override
  public boolean isStopped() {
    throw new NotImplementedException("This method is not implemented");
  }

  @Override
  public JobStatus findStatus(String id, String contextId) {
    throw new NotImplementedException("This method is not implemented");
  }

}