package org.rabix.engine.store.cache;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.ehcache.spi.loaderwriter.BulkCacheLoadingException;
import org.ehcache.spi.loaderwriter.BulkCacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.rabix.bindings.model.Job;
import org.rabix.engine.store.postgres.jdbi.impl.JDBIJobRepository;

public class JobsCacheWriter implements CacheLoaderWriter<UUID, HashSet> {

  private JDBIJobRepository repo;

  public JobsCacheWriter(JDBIJobRepository repo) {
    this.repo = repo;
  }

  @Override
  public HashSet<Job> load(UUID key) throws Exception {
    return (HashSet<Job>) repo.getReadyJobsByGroupId(key);
  }

  @Override
  public Map<UUID, HashSet> loadAll(Iterable<? extends UUID> keys) throws BulkCacheLoadingException, Exception {
    Map<UUID, HashSet> out = new HashMap<>();
    keys.forEach(key -> {
      try {
        out.put(key, load(key));
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
    return out;
  }

  @Override
  public void write(UUID key, HashSet value) throws Exception {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void writeAll(Iterable<? extends Entry<? extends UUID, ? extends HashSet>> entries) throws BulkCacheWritingException, Exception {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void delete(UUID key) throws Exception {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void deleteAll(Iterable<? extends UUID> keys) throws BulkCacheWritingException, Exception {
    // TODO Auto-generated method stub
    
  }

}
