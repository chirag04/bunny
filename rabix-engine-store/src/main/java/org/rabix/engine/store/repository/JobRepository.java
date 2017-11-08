package org.rabix.engine.store.repository;

import java.sql.Timestamp;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import org.rabix.bindings.model.Job;
import org.rabix.bindings.model.Job.JobStatus;

public interface JobRepository {

  void insert(Job job, UUID groupId, String producedByNode);
  
  void insert(Iterator<Job> jobs);

  void update(Job job);
  
  void update(Iterator<Job> jobs);
  
  void updateStatus(UUID rootId, JobStatus status, Set<JobStatus> whereStatuses);
    
  Job get(UUID id);
  
  Set<Job> getRootJobsForDeletion(JobStatus status, Timestamp olderThanTime);
  
  Set<Job> getReadyJobsByGroupId(UUID groupId);
    
  void deleteByRootIds(Set<UUID> rootIds);
  
  default void updatePartial(Job job) {
    update(job);
  }
  
  public class JobEntity {
    
    Job job;
    UUID groupId;
    String producedByNode;
    UUID backendId;
    
    public JobEntity(Job job, UUID groupId, String producedByNode, UUID backendId) {
      super();
      this.job = job;
      this.groupId = groupId;
      this.backendId = backendId;
      this.producedByNode = producedByNode;
    }

    public JobEntity(Job job, UUID groupId, String producedByNode) {
      super();
      this.job = job;
      this.groupId = groupId;
      this.producedByNode = producedByNode;
      this.backendId = null;
    }

    public Job getJob() {
      return job;
    }

    public void setJob(Job job) {
      this.job = job;
    }

    public UUID getGroupId() {
      return groupId;
    }
    
    public String getProducedByNode() {
      return producedByNode;
    }
    
    public void setGroupId(UUID groupId) {
      this.groupId = groupId;
    }

    public UUID getBackendId() {
      return backendId;
    }

    public void setBackendId(UUID backendId) {
      this.backendId = backendId;
    }

    public void setProducedByNode(String producedByNode) {
      this.producedByNode = producedByNode;
    }
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((job == null) ? 0 : job.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      JobEntity other = (JobEntity) obj;
      if (job == null) {
        if (other.job != null)
          return false;
      } else if (!job.equals(other.job))
        return false;
      return true;
    }

  }

}