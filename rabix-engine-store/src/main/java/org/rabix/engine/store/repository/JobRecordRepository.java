package org.rabix.engine.store.repository;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.rabix.engine.store.model.JobRecord;

public interface JobRecordRepository {

  public void insert(JobRecord jobRecord);

  public void update(JobRecord jobRecord);

  public JobRecord get(String id, UUID rootId);

  public void deleteByStatus(UUID rootId, JobRecord.JobState state);
  
  public List<JobRecord> getByParent(UUID parentId, UUID rootId);

  public List<JobRecord> get(UUID rootId, Set<JobRecord.JobState> states);

  public List<JobRecord> getAll(UUID rootId);
  
  
//public List<JobRecord> getReady(UUID rootId);
//public void delete(Set<JobIdRootIdPair> externalIDs);
//public void insertBatch(Iterator<JobRecord> records);
//public void updateBatch(Iterator<JobRecord> records);
//public void updateStatus(UUID rootId, JobRecord.JobState state, Set<JobRecord.JobState> whereStates);
//public JobRecord getRoot(UUID rootId);
}
