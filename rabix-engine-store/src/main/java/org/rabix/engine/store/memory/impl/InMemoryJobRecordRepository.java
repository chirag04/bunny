package org.rabix.engine.store.memory.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.rabix.engine.store.model.JobRecord;
import org.rabix.engine.store.repository.JobRecordRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;

public class InMemoryJobRecordRepository implements JobRecordRepository {

  private final static Logger logger = LoggerFactory.getLogger(InMemoryJobRecordRepository.class);

  Map<UUID, Map<UUID, JobRecord>> jobRecordsPerRoot;

  @Inject
  public InMemoryJobRecordRepository() {
    this.jobRecordsPerRoot = new ConcurrentHashMap<UUID, Map<UUID, JobRecord>>();
  }

  @Override
  public synchronized void insert(JobRecord jobRecord) {
    Map<UUID, JobRecord> rootJobs = jobRecordsPerRoot.get(jobRecord.getRootId());
    if (rootJobs == null) {
      rootJobs = new HashMap<UUID, JobRecord>();
      jobRecordsPerRoot.put(jobRecord.getRootId(), rootJobs);
    }
    rootJobs.put(jobRecord.getExternalId(), jobRecord);
  }

  @Override
  public synchronized void update(JobRecord jobRecord) {
    Map<UUID, JobRecord> rootJobs = jobRecordsPerRoot.get(jobRecord.getRootId());
    if (rootJobs == null) {
      rootJobs = new HashMap<UUID, JobRecord>();
      jobRecordsPerRoot.put(jobRecord.getRootId(), rootJobs);
    }
    rootJobs.put(jobRecord.getExternalId(), jobRecord);
  }

//  @Override
//  public synchronized void insertBatch(Iterator<JobRecord> records) {
//    while (records.hasNext()) {
//      JobRecord jobRecord = records.next();
//      insert(jobRecord);
//    }
//  }
//
//  @Override
//  public synchronized void updateBatch(Iterator<JobRecord> records) {
//    while (records.hasNext()) {
//      JobRecord jobRecord = records.next();
//      update(jobRecord);
//    }
//  }

  @Override
  public synchronized void deleteByStatus(UUID rootId, JobRecord.JobState state) {
    
  }

//  @Override
//  public synchronized void delete(Set<JobIdRootIdPair> externalIDs) {
//    for (JobIdRootIdPair job : externalIDs) {
//      jobRecordsPerRoot.get(job.rootId).remove(job.id);
//      if (job.id.equals(job.rootId)) {
//        jobRecordsPerRoot.remove(job.rootId);
//      }
//    }
//  }
//
  @Override
  public synchronized List<JobRecord> getAll(UUID rootId) {
    Map<UUID, JobRecord> recordsPerRoot = jobRecordsPerRoot.get(rootId);
    if (recordsPerRoot == null) {
      return new ArrayList<>();
    }
    List<JobRecord> records = new ArrayList<>();
    records.addAll(recordsPerRoot.values());
    return records;
  }
//
//  @Override
//  public synchronized JobRecord getRoot(UUID rootId) {
//    Map<UUID, JobRecord> recordsPerRoot = jobRecordsPerRoot.get(rootId);
//    return recordsPerRoot != null ? recordsPerRoot.get(rootId) : null;
//  }

  @Override
  public synchronized JobRecord get(String id, UUID rootId) {
    Map<UUID, JobRecord> recordsPerRoot = jobRecordsPerRoot.get(rootId);
    if (recordsPerRoot != null) {
      for (JobRecord job : jobRecordsPerRoot.get(rootId).values()) {
        if (job.getId().equals(id)) {
          return job;
        }
      }
    }
    logger.debug("Failed to find jobRecord {} for root {}", id, rootId);
    return null;
  }

  @Override
  public synchronized List<JobRecord> getByParent(UUID parentId, UUID rootId) {
    List<JobRecord> jobsByParent = new ArrayList<JobRecord>();
    if (jobRecordsPerRoot.get(rootId) != null) {
      for (JobRecord job : jobRecordsPerRoot.get(rootId).values()) {
        if (job.getParentId() != null && job.getParentId().equals(parentId)) {
          jobsByParent.add(job);
        }
      }
    }
    return jobsByParent;
  }


//  @Override
//  public void updateStatus(UUID rootId, JobRecord.JobState state, Set<JobRecord.JobState> whereStates) {
//    Map<UUID, JobRecord> jobs = jobRecordsPerRoot.get(rootId);
//    jobs.values().stream().filter(p -> whereStates.contains(p.getState())).forEach(p -> {
//      p.setState(state);
//    });
//  }

  @Override
  public List<JobRecord> get(UUID rootId, Set<JobRecord.JobState> states) {
    Map<UUID, JobRecord> jobs = jobRecordsPerRoot.get(rootId);
    return jobs.values().stream().filter(p -> states.contains(p.getState())).collect(Collectors.toList());
  }

}
