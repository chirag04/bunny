package org.rabix.engine.store.cache;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.rabix.engine.store.model.JobRecord;
import org.rabix.engine.store.model.JobRecord.JobState;
import org.rabix.engine.store.postgres.jdbi.impl.JDBIJobRecordRepository;
import org.rabix.engine.store.repository.JobRecordRepository;

public class JobRecordCachedRepository implements JobRecordRepository {

  private JDBIJobRecordRepository repo;
  private Cache<JobRecordKey, JobRecord> cache;

  public static final String CACHE = "jobRecordCache";

  public JobRecordCachedRepository(JDBIJobRecordRepository repo, CacheManager manager) {
    this.repo = repo;
    this.cache = manager.getCache(JobRecordCachedRepository.CACHE, JobRecordKey.class, JobRecord.class);
    if (cache == null)
      this.cache = manager.createCache(JobRecordCachedRepository.CACHE,
          CacheConfigurationBuilder.newCacheConfigurationBuilder(JobRecordKey.class, JobRecord.class, ResourcePoolsBuilder.heap(1000))
              .withLoaderWriter(new JobRecordCacheWriter(repo)).build());
  }

  @Override
  public void insert(JobRecord jobRecord) {
    cache.put(new JobRecordKey(jobRecord.getId(), jobRecord.getRootId()), jobRecord);
  }

  @Override
  public void update(JobRecord jobRecord) {
    cache.put(new JobRecordKey(jobRecord.getId(), jobRecord.getRootId()), jobRecord);
  }

  @Override
  public JobRecord get(String id, UUID rootId) {
    return cache.get(new JobRecordKey(id, rootId));
  }

  @Override
  public void deleteByStatus(UUID rootId, JobState state) {
    repo.deleteByStatus(rootId, state);
  }


  @Override
  public List<JobRecord> getByParent(UUID parentId, UUID rootId) {
    return repo.getByParent(parentId, rootId);
  }

  @Override
  public List<JobRecord> get(UUID rootId, Set<JobState> states) {
    return repo.get(rootId, states);
  }

  @Override
  public List<JobRecord> getAll(UUID rootId) {
    return repo.getAll(rootId);
  }

  public static class JobRecordKey implements Serializable{
    private static final long serialVersionUID = 8379824235187260546L;
    private String name;
    private UUID rootId;

    public JobRecordKey(String name, UUID rootId) {
      super();
      this.name = name;
      this.rootId = rootId;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public void setRootId(UUID rootId) {
      this.rootId = rootId;
    }

    public UUID getRootId() {
      return rootId;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((name == null) ? 0 : name.hashCode());
      result = prime * result + ((rootId == null) ? 0 : rootId.hashCode());
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
      JobRecordKey other = (JobRecordKey) obj;
      if (name == null) {
        if (other.name != null)
          return false;
      } else if (!name.equals(other.name))
        return false;
      if (rootId == null) {
        if (other.rootId != null)
          return false;
      } else if (!rootId.equals(other.rootId))
        return false;
      return true;
    }

  }
}
