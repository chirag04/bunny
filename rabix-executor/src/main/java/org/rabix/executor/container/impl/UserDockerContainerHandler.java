package org.rabix.executor.container.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.spotify.docker.client.LogStream;
import com.spotify.docker.client.exceptions.DockerException;

/**
 * Docker based implementation of {@link ContainerHandler}
 */
public class UserDockerContainerHandler implements ContainerHandler {

  private static final Logger logger = LoggerFactory.getLogger(UserDockerContainerHandler.class);

  public static final String DIRECTORY_MAP_MODE = "rw";

  public static final String HOME_ENV_VAR = "HOME";
  public static final String TMPDIR_ENV_VAR = "TMPDIR";

  private static final String TAG_SEPARATOR = ":";
  private static final String LATEST = "latest";
  private static final String SEPARATOR = " ";

  private String containerId;
  private final Job job;
  private final DockerContainerRequirement dockerResource;
  private final File workingDir;
  private Integer overrideResultStatus = null;


  private String commandLine;

  private final String dockerOverride;
  private boolean running;
  private String pullCommand;

  public UserDockerContainerHandler(Job job, Configuration configuration, DockerContainerRequirement dockerResource, StorageConfiguration storageConfig,
      DockerConfigation dockerConfig, WorkerStatusCallback statusCallback, String override) throws ContainerException {
    this.job = job;
    this.dockerResource = dockerResource;
    this.workingDir = storageConfig.getWorkingDir(job);
    this.dockerOverride = override;
    this.pullCommand = configuration.getString("executor.override.pull");
  }

  private void pull(String image) throws ContainerException {
    if (pullCommand != null) {
      ProcessBuilder processBuilder = new ProcessBuilder();
      String replaced = pullCommand.replace("{image}", image);
      processBuilder.command(replaced.split(SEPARATOR));
      Process pull;
      try {
        pull = processBuilder.start();
        pull.waitFor();
      } catch (IOException | InterruptedException e) {
        logger.error("Failed to pull image with command: {}", replaced, e);
      }
    }
  }

  private String checkTagOrAddLatest(String image) {
    return image.contains(TAG_SEPARATOR) ? image : image + TAG_SEPARATOR + LATEST;
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

    public String build() {
      String out = dockerOverride;
      out = match(out, "image", Collections.singleton(image));
      out = match(out, "vols", volumes);
      out = match(out, "envs", env);
      out = match(out, "command", Collections.singleton(cmd.trim()));
      out = match(out, "workdir", Collections.singleton(workingDir));
      return out;
    }

    private String match(String value, String name, Collection<String> col) {
      Pattern pattern = Pattern.compile("(\\{([^\\|\\{]*)\\|?" + name + "\\})");
      Matcher matcher = pattern.matcher(value);
      if (!matcher.find())
        return value;
      String prefix = matcher.group(2);
      final StringBuilder prefixed = new StringBuilder();
      col.forEach(v -> prefixed.append(prefix + SEPARATOR + v + SEPARATOR));
      return value.replaceAll(pattern.pattern(), prefixed.toString().trim());
    }
  }

  @Override
  public void start() throws ContainerException {
    String dockerPull = checkTagOrAddLatest(dockerResource.getDockerPull());

    try {
      pull(dockerPull);
      UserBuilder builder = new UserBuilder();
      builder.image(dockerResource.getDockerPull().split(":")[1]);

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

      // commandLine = addEntrypoint(entrypoint, commandLine); TODO:entrypoint overwrite

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
      String shFile = "/tmp/" + job.getId() + ".sh";
      Files.write(Paths.get(shFile), ("/bin/sh -c \"" + commandLine + "\"").getBytes());
      builder.cmd("/bin/sh " + shFile);
      ProcessBuilder processBuilder = new ProcessBuilder();
      processBuilder.command(builder.build().split(" "));
      Process process = processBuilder.start();
      process.waitFor();
      running = false;
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

  // private String addEntrypoint(List<String> entrypoint, String commandLine) {
  // if (entrypoint != null && !entrypoint.isEmpty()) {
  // String entryPointString = null;
  // for (String part : entrypoint) {
  // if (entryPointString == null) {
  // entryPointString = part + " ";
  // } else {
  // entryPointString = entryPointString + part + SEPARATOR;
  // }
  // }
  // commandLine = entryPointString + commandLine;
  // }
  // return commandLine;
  // }

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
  public void stop() throws ContainerException {
    // if (overrideResultStatus != null) {
    // return;
    // }
    // try {
    // dockerClient.stopContainer(containerId, 0);
    // } catch (Exception e) {
    // logger.error("Docker container " + containerId + " failed to stop", e);
    // throw new ContainerException("Docker container " + containerId + " failed to stop");
    // }
  }

  @JsonIgnore
  public boolean isStarted() throws ContainerException {
    // if (overrideResultStatus != null) {
    // return true;
    // }
    // ContainerInfo containerInfo;
    // try {
    // containerInfo = dockerClient.inspectContainer(containerId);
    // ContainerState containerState = containerInfo.state();
    // Date startedDate = containerState.startedAt();
    // return startedDate != null;
    // } catch (Exception e) {
    // logger.error("Failed to query docker. Container ID: " + containerId, e);
    // throw new ContainerException("Failed to query docker. Container ID: " + containerId);
    // }
    return true;
  }

  @Override
  @JsonIgnore
  public boolean isRunning() throws ContainerException {
    return running;
  }

  @Override
  @JsonIgnore
  public int getProcessExitStatus() throws ContainerException {
    // if (overrideResultStatus != null) {
    // return overrideResultStatus;
    // }
    // ContainerInfo containerInfo;
    // try {
    // containerInfo = dockerClient.inspectContainer(containerId);
    // ContainerState containerState = containerInfo.state();
    // return containerState.exitCode();
    // } catch (Exception e) {
    // logger.error("Failed to query docker. Container ID: " + containerId, e);
    // throw new ContainerException("Failed to query docker. Container ID: " + containerId);
    // }
    return 0;
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
        dumpLog(containerId, logFile);
      } catch (Exception e) {
        logger.error("Docker container " + containerId + " failed to create log file", e);
        throw new ContainerException("Docker container " + containerId + " failed to create log file");
      }
    }
  }

  @Override
  public void removeContainer() {

  }

  /**
   * Helper method for dumping error logs from Docker to file
   */
  public void dumpLog(String containerId, File logFile) throws DockerException, InterruptedException {
    LogStream errorStream = null;

    FileChannel fileChannel = null;
    FileOutputStream fileOutputStream = null;
    try {
      if (logFile.exists()) {
        logFile.delete();
      }
      logFile.createNewFile();

      fileOutputStream = new FileOutputStream(logFile);
      fileChannel = fileOutputStream.getChannel();

      // errorStream = dockerClient.logs(containerId, LogsParam.stderr());
      // while (errorStream.hasNext()) {
      // LogMessage message = errorStream.next();
      // ByteBuffer buffer = message.content();
      // fileChannel.write(buffer);
      // }
    } catch (FileNotFoundException e) {
      throw new DockerException("File " + logFile + " not found");
    } catch (IOException e) {
      throw new DockerException(e);
    } finally {
      if (errorStream != null) {
        errorStream.close();
      }
      if (fileChannel != null) {
        try {
          fileChannel.close();
        } catch (IOException e) {
          logger.error("Failed to close file channel", e);
        }
      }
      if (fileOutputStream != null) {
        try {
          fileOutputStream.close();
        } catch (IOException e) {
          logger.error("Failed to close file output stream", e);
        }
      }
    }
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
