package org.rabix.engine.store.memory.impl;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.rabix.bindings.model.Job;
import org.rabix.bindings.model.Job.JobStatus;
import org.rabix.engine.store.repository.JobRepository;

import com.google.inject.Inject;

public class InMemoryJobRepository implements JobRepository {

  Map<UUID, Map<UUID, JobEntity>> jobRepository;
  
  @Inject
  public InMemoryJobRepository() {
    this.jobRepository = new ConcurrentHashMap<UUID, Map<UUID, JobEntity>>();
  }

  @Override
  public synchronized void insert(Job job, UUID groupId, String producedByNode) {
    Map<UUID, JobEntity> rootJobs = jobRepository.get(job.getRootId());
    if(rootJobs == null) {
      rootJobs = new HashMap<UUID, JobEntity>();
      rootJobs.put(job.getId(), new JobEntity(job, groupId));
      jobRepository.put(job.getRootId(), rootJobs);
    }
    else {
      rootJobs.put(job.getId(), new JobEntity(job, groupId));
    }
  }

  @Override
  public synchronized void update(Job job) {
    Map<UUID, JobEntity> rootJobs = jobRepository.get(job.getRootId());
    rootJobs.get(job.getId()).setJob(job);
  }

  @Override
  public synchronized Job get(UUID id) {
    return getJobEntity(id) != null ? getJobEntity(id).getJob(): null;
  }

  @Override
  public synchronized Set<Job> getReadyJobsByGroupId(UUID groupId) {
    Set<Job> groupIdJobs = new HashSet<Job>();
    for(Map<UUID, JobEntity> rootJobs: jobRepository.values()) {
      for(JobEntity job: rootJobs.values()) {
        if(job.getGroupId() != null && job.getGroupId().equals(groupId) && job.getJob().getStatus().equals(JobStatus.READY)) {
          groupIdJobs.add(job.getJob());
        }
      }
    }
    return groupIdJobs;
  }
  
  private JobEntity getJobEntity(UUID jobId) {
    for(Map<UUID, JobEntity> rootJobs: jobRepository.values()) {
      if(rootJobs.get(jobId) != null) {
        return rootJobs.get(jobId);
      }
    }
    return null;
  }

  @Override
  public Set<Job> getRootJobsForDeletion(JobStatus status, Timestamp time) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void deleteByRootIds(Set<UUID> rootIds) {
    // TODO Auto-generated method stub
  }
}
