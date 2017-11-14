package org.rabix.engine.service;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.rabix.bindings.model.Job;

public interface JobService {

  void update(Job job) throws JobServiceException;

  Job start(Job job, Map<String, Object> config) throws JobServiceException;
  
  void stop(UUID id) throws JobServiceException;
  
  void delete(UUID jobId);
  
  Job get(UUID id);

  void handleJobCompleted(Job job);

  void handleJobRootPartiallyCompleted(UUID uuid, Map<String, Object> outputs, String producedBy);

  void handleJobRootFailed(Job job);

  void handleJobRootCompleted(Job job);

  void handleJobFailed(Job failedJob);

  void handleJobsReady(Set<Job> jobs, UUID rootId, String producedByNode);

  void handleJobContainerReady(Job containerJob);

  void handleJobRootAborted(Job rootJob);

}
