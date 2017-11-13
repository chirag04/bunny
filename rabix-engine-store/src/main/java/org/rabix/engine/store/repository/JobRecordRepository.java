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

}
