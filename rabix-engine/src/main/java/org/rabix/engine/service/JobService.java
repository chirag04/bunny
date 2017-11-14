package org.rabix.engine.service;

import java.util.Map;
import java.util.UUID;

import org.rabix.bindings.model.Job;

public interface JobService {

  void update(Job job) throws JobServiceException;

  Job start(Job job, Map<String, Object> config) throws JobServiceException;
  
  void stop(UUID id) throws JobServiceException;
  
  void delete(UUID jobId);
  
  Job get(UUID id);
}
