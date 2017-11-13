package org.rabix.engine.store.cache;

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.rabix.bindings.model.Job;
import org.rabix.bindings.model.Job.JobStatus;
import org.rabix.engine.store.postgres.jdbi.impl.JDBIJobRepository;
import org.rabix.engine.store.repository.JobRepository;

public class JobCachedRepository implements JobRepository {

  private JDBIJobRepository repo;
  private Cache<UUID, HashSet> setCache;

  public static final String SET_CACHE = "jobsCache";
  public static final String CACHE = "jobCache";

  public JobCachedRepository(JDBIJobRepository repo, CacheManager manager) {
    this.repo = repo;
    this.setCache = manager.getCache(JobCachedRepository.SET_CACHE, UUID.class, HashSet.class);
    if (setCache == null) {
      this.setCache = manager.createCache(JobCachedRepository.SET_CACHE,
          CacheConfigurationBuilder.newCacheConfigurationBuilder(UUID.class, HashSet.class, ResourcePoolsBuilder.heap(1000)).build());
    }
  }

  @Override
  public Set<Job> getReadyJobsByGroupId(UUID groupId) {
    HashSet out = setCache.get(groupId);
    setCache.remove(groupId);
    return out == null ? new HashSet() : out;
  }

  @Override
  public void insert(Job job, UUID groupId, String producedByNode) {
    if (job.getId().equals(groupId)) {
      repo.insert(job, groupId, producedByNode);
    } else {
      HashSet set = setCache.get(groupId);
      if (set == null) {
        set = new HashSet<>();
      }
      set.add(job);
      setCache.put(groupId, set);
    }
  }

  @Override
  public void update(Job job) {
    repo.update(job);
  }

  @Override
  public void updatePartial(Job job) {
    repo.updatePartial(job);
  }

  @Override
  public Job get(UUID id) {
    return repo.get(id);
  }

  @Override
  public Set<Job> getRootJobsForDeletion(JobStatus status, Timestamp olderThanTime) {
    return repo.getRootJobsForDeletion(status, olderThanTime);
  }

  @Override
  public void deleteByRootIds(Set<UUID> rootIds) {
    repo.deleteByRootIds(rootIds);
  }
}
