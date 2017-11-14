package org.rabix.engine.store.cache;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.ehcache.spi.loaderwriter.BulkCacheLoadingException;
import org.ehcache.spi.loaderwriter.BulkCacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.rabix.engine.store.cache.JobRecordCachedRepository.JobRecordKey;
import org.rabix.engine.store.model.JobRecord;
import org.rabix.engine.store.postgres.jdbi.impl.JDBIJobRecordRepository;

public class JobRecordCacheWriter implements CacheLoaderWriter<JobRecordKey, JobRecord> {

  private JDBIJobRecordRepository repo;

  public JobRecordCacheWriter(JDBIJobRecordRepository repo) {
    this.repo = repo;
  }

  @Override
  public JobRecord load(JobRecordKey key) throws Exception {
    return repo.get(key.getName(), key.getRootId());
  }

  @Override
  public Map<JobRecordKey, JobRecord> loadAll(Iterable<? extends JobRecordKey> keys) throws BulkCacheLoadingException, Exception {
    Map<JobRecordKey, JobRecord> out = new HashMap<>();
    keys.forEach(key -> out.put(key, repo.get(key.getName(), key.getRootId())));
    return out;
  }

  @Override
  public void write(JobRecordKey key, JobRecord value) throws Exception {
    repo.insert(value);
  }

  @Override
  public void writeAll(Iterable<? extends Entry<? extends JobRecordKey, ? extends JobRecord>> entries) throws BulkCacheWritingException, Exception {
    entries.forEach(e -> repo.insert(e.getValue()));
  }

  @Override
  public void delete(JobRecordKey key) throws Exception {}

  @Override
  public void deleteAll(Iterable<? extends JobRecordKey> keys) throws BulkCacheWritingException, Exception {}
}
