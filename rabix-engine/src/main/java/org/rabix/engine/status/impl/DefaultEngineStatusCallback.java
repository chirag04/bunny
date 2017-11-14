package org.rabix.engine.status.impl;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.rabix.bindings.model.Job;
import org.rabix.engine.service.Scheduler;
import org.rabix.engine.status.EngineStatusCallback;
import org.rabix.engine.status.EngineStatusCallbackException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;

public class DefaultEngineStatusCallback implements EngineStatusCallback {

  private final static Logger logger = LoggerFactory.getLogger(DefaultEngineStatusCallback.class);
  
  @Inject
  private Scheduler scheduler;
  
  @Override
  public void onJobReady(Job job) throws EngineStatusCallbackException {
    logger.debug("onJobReady(jobId={})", job.getId());
  }
  
  @Override
  public void onJobsReady(Set<Job> jobs, UUID rootId, String producedByNode) throws EngineStatusCallbackException {
    scheduler.scheduleJobs(jobs);
  }

  @Override
  public void onJobFailed(String name, UUID rootId, String result) throws EngineStatusCallbackException {
    logger.debug("onJobFailed(jobId={})", name);
  }

  @Override
  public void onJobCompleted(String name, UUID rootId, Object result) throws EngineStatusCallbackException {
    logger.debug("onJobCompleted(jobId={})", name);
  }
  
  @Override
  public void onJobRootCompleted(Job rootJob) throws EngineStatusCallbackException {
    logger.debug("onJobRootCompleted(jobId={})", rootJob.getId());
  }
  
  @Override
  public void onJobRootPartiallyCompleted(UUID rootId, Map<String,Object> outputs, String producedBy) throws EngineStatusCallbackException {
    logger.debug("onJobRootPartiallyCompleted(jobId={})", rootId);
  }

  @Override
  public void onJobRootFailed(Job rootJob) throws EngineStatusCallbackException {
    logger.debug("onJobFailed(jobId={})", rootJob.getId());
  }
  
  @Override
  public void onJobRootAborted(Job rootJob) throws EngineStatusCallbackException {
    logger.debug("onJobAborted(jobId={})", rootJob.getId());
  }

  @Override
  public void onJobRootRunning(UUID rootId) throws EngineStatusCallbackException {
    // TODO Auto-generated method stub
    
  }

}
