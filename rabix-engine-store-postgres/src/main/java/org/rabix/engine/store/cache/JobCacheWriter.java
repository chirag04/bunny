package org.rabix.engine.store.cache;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.ehcache.spi.loaderwriter.BulkCacheLoadingException;
import org.ehcache.spi.loaderwriter.BulkCacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.rabix.bindings.model.Job;
import org.rabix.engine.store.postgres.jdbi.impl.JDBIJobRepository;

public class JobCacheWriter implements CacheLoaderWriter<UUID, Job> {

  private JDBIJobRepository repo;

  public JobCacheWriter(JDBIJobRepository repo) {
    this.repo = repo;
  }

  @Override
  public Job load(UUID key) throws Exception {
    return repo.get(key);
  }

  @Override
  public void deleteAll(Iterable<? extends UUID> keys) throws BulkCacheWritingException, Exception {
    // TODO Auto-generated method stub
    
  }

  @Override
  public Map<UUID, Job> loadAll(Iterable<? extends UUID> keys) throws BulkCacheLoadingException, Exception {
    Map<UUID, Job> jobs = new HashMap<>();
    for(UUID key:keys)
      jobs.put(key, load(key));
    return jobs;
  }

  @Override
  public void write(UUID key, Job value) throws Exception {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void writeAll(Iterable<? extends Entry<? extends UUID, ? extends Job>> entries) throws BulkCacheWritingException, Exception {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void delete(UUID key) throws Exception {
    // TODO Auto-generated method stub
    
  }

}
