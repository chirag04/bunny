package org.rabix.executor.container.impl;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.rabix.backend.api.callback.WorkerStatusCallback;
import org.rabix.bindings.Bindings;
import org.rabix.bindings.BindingsFactory;
import org.rabix.bindings.helper.FileValueHelper;
import org.rabix.bindings.model.Job;
import org.rabix.bindings.model.Resources;
import org.rabix.bindings.model.requirement.DockerContainerRequirement;
import org.rabix.bindings.model.requirement.EnvironmentVariableRequirement;
import org.rabix.bindings.model.requirement.Requirement;
import org.rabix.executor.config.DockerConfigation;
import org.rabix.executor.config.StorageConfiguration;
import org.rabix.executor.container.ContainerException;
import org.rabix.executor.container.ContainerHandler;
import org.rabix.executor.handler.JobHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Docker based implementation of {@link ContainerHandler}
 */
public class UserDockerContainerHandler implements ContainerHandler {

  private static final Logger logger = LoggerFactory.getLogger(UserDockerContainerHandler.class);

  public static final String DIRECTORY_MAP_MODE = "rw";

  public static final String EXECUTOR_OVERRIDE_COMMAND = "executor.override.command";
  public static final String EXECUTOR_OVERRIDE_SETUP = "executor.override.setup";

  public static final String HOME_ENV_VAR = "HOME";
  public static final String TMPDIR_ENV_VAR = "TMPDIR";
  private static final String SEPARATOR = " ";

  private final Job job;
  private final DockerContainerRequirement dockerResource;
  private final File workingDir;
  private Integer overrideResultStatus = null;

  private Future<Integer> processFuture;
  private ExecutorService executorService = Executors.newSingleThreadExecutor();

  private String commandLine;

  private final String dockerOverride;
  private final String dockerOverrideSetup;
  private String errorLog;


  public UserDockerContainerHandler(Job job, Configuration configuration, DockerContainerRequirement dockerResource, StorageConfiguration storageConfig,
      DockerConfigation dockerConfig, WorkerStatusCallback statusCallback) throws ContainerException {
    this.job = job;
    this.dockerResource = dockerResource;
    this.workingDir = storageConfig.getWorkingDir(job);
    this.dockerOverride = configuration.getString(EXECUTOR_OVERRIDE_COMMAND);
    this.dockerOverrideSetup = configuration.getString(EXECUTOR_OVERRIDE_SETUP);
  }

  private class UserBuilder {
    String image;
    Set<String> volumes = new HashSet<>();
    String workingDir;
    String cmd;
    List<String> env;

    public UserBuilder image(String image) {
      this.image = image;
      return this;
    }

    public UserBuilder workingDir(String workingDir) {
      this.workingDir = workingDir;
      return this;
    }

    public UserBuilder cmd(String cmd) {
      this.cmd = cmd;
      return this;
    }

    public UserBuilder volume(String volume) {
      this.volumes.add(volume);
      return this;
    }

    public UserBuilder env(List<String> env) {
      this.env = env;
      return this;
    }

    public String build(String base) {
      String out = base;
      out = match(out, "image", Collections.singleton(image));
      out = match(out, "vols", volumes);
      out = match(out, "envs", env);
      out = match(out, "command", Collections.singleton(cmd.trim()));
      out = match(out, "workdir", Collections.singleton(workingDir));
      return out;
    }

    private String match(String value, String name, Collection<String> col) {
      Pattern pattern = Pattern.compile("\\{" + name + "(\\|prefix:(\\S*))?\\}");
      Matcher matcher = pattern.matcher(value);
      if (!matcher.find())
        return value;
      String prefix = matcher.group(2);
      final StringBuilder prefixed = new StringBuilder();
      col.forEach(v -> prefixed.append((prefix == null ? "" : prefix + SEPARATOR) + v + SEPARATOR));
      return value.replaceAll(pattern.pattern(), prefixed.toString().trim());
    }
  }

  @Override
  public void start() throws ContainerException {
    try {
      UserBuilder builder = new UserBuilder();
      builder.image(dockerResource.getDockerPull());

      FileValueHelper.getInputFiles(job).forEach(f -> {
        builder.volume(URI.create(f.getLocation()).getPath() + ":" + f.getPath());
        f.getSecondaryFiles().forEach(sec -> builder.volume(URI.create(sec.getLocation()).getPath() + ":" + sec.getPath()));
      });
      if (dockerResource.getDockerOutputDirectory() != null) {
        builder.volume(workingDir.getAbsolutePath() + ":" + dockerResource.getDockerOutputDirectory());
      } else {
        builder.volume(workingDir.getAbsolutePath() + ":" + workingDir.getAbsolutePath());
      }


      Bindings bindings = BindingsFactory.create(job);
      commandLine = bindings.buildCommandLineObject(job, workingDir, (String path, Map<String, Object> config) -> {
        return path;
      }).build();

      if (StringUtils.isEmpty(commandLine.trim())) {
        overrideResultStatus = 0; // default is success
        return;
      }

      if (commandLine.startsWith("/bin/bash -c")) {
        commandLine = normalizeCommandLine(commandLine.replace("/bin/bash -c", ""));
      } else if (commandLine.startsWith("/bin/sh -c")) {
        commandLine = normalizeCommandLine(commandLine.replace("/bin/sh -c", ""));
      } else {
        commandLine = normalizeCommandLine(commandLine);
      }

      builder.workingDir(workingDir.getAbsolutePath());

      List<Requirement> combinedRequirements = new ArrayList<>();
      combinedRequirements.addAll(bindings.getHints(job));
      combinedRequirements.addAll(bindings.getRequirements(job));

      EnvironmentVariableRequirement environmentVariableResource = getRequirement(combinedRequirements, EnvironmentVariableRequirement.class);
      Map<String, String> environmentVariables = environmentVariableResource != null ? environmentVariableResource.getVariables()
          : new HashMap<String, String>();
      Resources resources = job.getResources();
      if (resources != null) {
        if (resources.getWorkingDir() != null) {
          environmentVariables.put(HOME_ENV_VAR, resources.getWorkingDir());
        }
        if (resources.getTmpDir() != null) {
          environmentVariables.put(TMPDIR_ENV_VAR, resources.getTmpDir());
        }
      }

      builder.env(transformEnvironmentVariables(environmentVariables));
      builder.cmd(commandLine);

      if (dockerOverrideSetup != null) {
        Process setup = new ProcessBuilder().command("/bin/sh", "-c", builder.build(dockerOverrideSetup)).start();
        setup.waitFor();
        if (setup.exitValue() != 0) {
          logger.error("Setup script encountered an error: {}", IOUtils.readLines(setup.getErrorStream()));
        }
      }

      ProcessBuilder processBuilder = new ProcessBuilder();
      processBuilder.command("/bin/sh", "-c", builder.build(dockerOverride));

      processFuture = executorService.submit(new Callable<Integer>() {
        @Override
        public Integer call() throws Exception {
          Process process = processBuilder.start();
          process.waitFor();
          List<String> lines = IOUtils.readLines(process.getErrorStream());
          if (lines != null && !lines.isEmpty()) {
            errorLog = lines.stream().reduce("", (a, b) -> a + b);
          }
          return process.exitValue();
        }
      });
    } catch (Exception e) {
      logger.error("Failed to start container.", e);
      throw new ContainerException("Failed to start container.", e);
    }
  }

  private String normalizeCommandLine(String commandLine) {
    commandLine = commandLine.trim();
    if (commandLine.startsWith("\"") && commandLine.endsWith("\"")) {
      commandLine = commandLine.substring(1, commandLine.length() - 1);
    }
    if (commandLine.startsWith("'") && commandLine.endsWith("'")) {
      commandLine = commandLine.substring(1, commandLine.length() - 1);
    }
    return commandLine;
  }

  private List<String> transformEnvironmentVariables(Map<String, String> variables) {
    List<String> transformed = new ArrayList<>();
    for (Entry<String, String> variableEntry : variables.entrySet()) {
      transformed.add(variableEntry.getKey() + "=" + variableEntry.getValue());
    }
    return transformed;
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

  @Override
  public synchronized void stop() throws ContainerException {
    if (processFuture == null) {
      return;
    }
    processFuture.cancel(true);
  }

  @Override
  public synchronized boolean isStarted() throws ContainerException {
    return processFuture != null;
  }

  @Override
  public synchronized boolean isRunning() throws ContainerException {
    if (processFuture == null) {
      return false;
    }
    return !processFuture.isDone();
  }

  @Override
  public synchronized int getProcessExitStatus() throws ContainerException {
    try {
      return processFuture.get();
    } catch (InterruptedException | ExecutionException e) {
      throw new ContainerException(e);
    }
  }

  /**
   * Does after processing (dumps standard error log for now)
   */
  @Override
  public void dumpContainerLogs(final File logFile) throws ContainerException {
    if (overrideResultStatus != null) {
      return;
    }
    logger.debug("Saving standard error files for id={}", job.getId());

    if (logFile != null) {
      try {
        Files.write(logFile.toPath(), errorLog == null ? new byte[] {} : errorLog.getBytes());
      } catch (Exception e) {
        logger.error("Failed to create log file", e);
        throw new ContainerException("Failed to create log file");
      }
    }
  }

  @Override
  public void removeContainer() {

  }


  @Override
  public void dumpCommandLine() throws ContainerException {
    try {
      File commandLineFile = new File(workingDir, JobHandler.COMMAND_LOG);
      FileUtils.writeStringToFile(commandLineFile, commandLine);
    } catch (IOException e) {
      logger.error("Failed to dump command line into " + JobHandler.COMMAND_LOG);
      throw new ContainerException(e);
    }
  }

}
