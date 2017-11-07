package org.rabix.engine.store.postgres.jdbi;

import java.sql.SQLException;
import java.util.UUID;
import java.util.logging.Logger;

import org.apache.commons.configuration.Configuration;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.impl.serialization.PlainJavaSerializer;
import org.postgresql.jdbc3.Jdbc3PoolingDataSource;
import org.rabix.engine.store.cache.JobRecordCacheWriter;
import org.rabix.engine.store.cache.JobRecordCachedRepository;
import org.rabix.engine.store.cache.JobRecordCachedRepository.JobRecordKey;
import org.rabix.engine.store.model.JobRecord;
import org.rabix.engine.store.postgres.jdbi.impl.JDBIJobRecordRepository;
import org.rabix.engine.store.repository.AppRepository;
import org.rabix.engine.store.repository.BackendRepository;
import org.rabix.engine.store.repository.ContextRecordRepository;
import org.rabix.engine.store.repository.DAGRepository;
import org.rabix.engine.store.repository.EventRepository;
import org.rabix.engine.store.repository.IntermediaryFilesRepository;
import org.rabix.engine.store.repository.JobRecordRepository;
import org.rabix.engine.store.repository.JobRepository;
import org.rabix.engine.store.repository.JobStatsRecordRepository;
import org.rabix.engine.store.repository.LinkRecordRepository;
import org.rabix.engine.store.repository.VariableRecordRepository;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.logging.SLF4JLog;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Named;

import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;

public class JDBIRepositoryModule extends AbstractModule {

  private static final String DBINIT_SQL = "org/rabix/engine/jdbi/dbinit.sql";

  public JDBIRepositoryModule() {}

  @Override
  public void configure() {
    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()//.withSerializer(JobRecordKey.class, PlainJavaSerializer.class)
        .build(true);
    bind(CacheManager.class).toInstance(cacheManager);
  }


  @Singleton
  @Provides
  @Named("cached")
  public boolean isCached(Configuration config) {
    return config.getBoolean("postgres.cached", true);
  }

  @Singleton
  @Provides
  public DBI provideDBI(Configuration configuration, Logger logger) {
    Jdbc3PoolingDataSource source = new Jdbc3PoolingDataSource();
    source.setDataSourceName("Data Source");
    source.setServerName(configuration.getString("postgres.server"));
    source.setSsl(configuration.getBoolean("postgres.ssl", true));
    source.setPortNumber(configuration.getInt("postgres.port"));
    source.setDatabaseName(configuration.getString("postgres.database"));
    source.setUser(configuration.getString("postgres.user"));
    source.setPassword(configuration.getString("postgres.password"));
    source.setMaxConnections(configuration.getInt("postgres.pool_max_connections"));

    try {
      JdbcConnection dbcon = new JdbcConnection(source.getConnection());
      Liquibase lb = new Liquibase(DBINIT_SQL, new ClassLoaderResourceAccessor(), dbcon);
      lb.update(new Contexts());
    } catch (SQLException e) {
      logger.severe(e.getMessage());
      System.exit(1);
    } catch (LiquibaseException e) {
      logger.severe(e.getMessage());
      System.exit(1);
    }

    DBI dbi = new DBI(source);
    dbi.setSQLLog(new SLF4JLog());
    return dbi;
  }

  @Singleton
  @Provides
  public JDBIRepositoryRegistry provideJDBIRepositoryRegistry(DBI dbi) {
    return dbi.onDemand(JDBIRepositoryRegistry.class);
  }

  @Provides
  public AppRepository provideAppRepository(JDBIRepositoryRegistry repositoryRegistry) {
    return repositoryRegistry.applicationRepository();
  }

  @Provides
  public BackendRepository provideBackendRepository(JDBIRepositoryRegistry repositoryRegistry) {
    return repositoryRegistry.backendRepository();
  }

  @Provides
  public DAGRepository provideDAGRepository(JDBIRepositoryRegistry repositoryRegistry) {
    return repositoryRegistry.dagRepository();
  }

  @Provides
  public JobRepository provideJobRepository(JDBIRepositoryRegistry repositoryRegistry) {
    return repositoryRegistry.jobRepository();
  }

  @Provides
  public EventRepository provideEventRepository(JDBIRepositoryRegistry repositoryRegistry) {
    return repositoryRegistry.eventRepository();
  }

  @Provides
  public JobRecordRepository provideJobRecordRepository(JDBIRepositoryRegistry repositoryRegistry, @Named("cached") boolean cached, CacheManager manager) {
    JDBIJobRecordRepository repo = repositoryRegistry.jobRecordRepository();
    if (cached) {
      return new JobRecordCachedRepository(repo, manager);
    } else {
      return repo;
    }
  }

  @Provides
  public LinkRecordRepository provideLinkRecordRepository(JDBIRepositoryRegistry repositoryRegistry, @Named("cached") boolean cached) {
    return repositoryRegistry.linkRecordRepository();
  }

  @Provides
  public VariableRecordRepository provideVariableRecordRepository(JDBIRepositoryRegistry repositoryRegistry, @Named("cached") boolean cached) {
    return repositoryRegistry.variableRecordRepository();
  }

  @Provides
  public ContextRecordRepository provideContextRecordRepository(JDBIRepositoryRegistry repositoryRegistry) {
    return repositoryRegistry.contextRecordRepository();
  }

  @Provides
  public JobStatsRecordRepository provideJobStatsRecordRepository(JDBIRepositoryRegistry repositoryRegistry) {
    return repositoryRegistry.jobStatsRecordRepository();
  }

  @Provides
  public IntermediaryFilesRepository provideIntermediaryFilesRepository(JDBIRepositoryRegistry repositoryRegistry) {
    return repositoryRegistry.intermediaryFilesRepository();
  }
}
