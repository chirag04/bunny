package org.rabix.engine.store.cache;

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Iterator;
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
  private Cache<UUID, Job> cache;

  public static final String SET_CACHE = "jobsCache";
  public static final String CACHE = "jobCache";

  public JobCachedRepository(JDBIJobRepository repo, CacheManager manager) {
    this.repo = repo;
    this.setCache = manager.getCache(JobCachedRepository.SET_CACHE, UUID.class, HashSet.class);
    if (setCache == null) {
      this.setCache = manager.createCache(JobCachedRepository.SET_CACHE, CacheConfigurationBuilder
          .newCacheConfigurationBuilder(UUID.class, HashSet.class, ResourcePoolsBuilder.heap(1000)).withLoaderWriter(new JobsCacheWriter(repo)).build());
    }
    this.cache = manager.getCache(JobCachedRepository.CACHE, UUID.class, Job.class);
    if (cache == null) {
      this.cache = manager.createCache(JobCachedRepository.CACHE, CacheConfigurationBuilder
          .newCacheConfigurationBuilder(UUID.class, Job.class, ResourcePoolsBuilder.heap(1000)).withLoaderWriter(new JobCacheWriter(repo)).build());
    }
  }

  @Override
  public Set<Job> getReadyJobsByGroupId(UUID groupId) {
     HashSet out = setCache.get(groupId);
     repo.insert(out.iterator());
     setCache.remove(groupId);
     return out;
  }

  @Override
  public void insert(Job job, UUID groupId, String producedByNode) {
    Set set = setCache.get(groupId);
    set.add(job);
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
  public void update(Iterator<Job> jobs) {
    repo.update(jobs);
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

  @Override
  public void insert(Iterator<Job> jobs) {
    repo.insert(jobs);
  }

  @Override
  public void updateStatus(UUID rootId, JobStatus status, Set<JobStatus> whereStatuses) {
    // TODO Auto-generated method stub
    
  }
}
