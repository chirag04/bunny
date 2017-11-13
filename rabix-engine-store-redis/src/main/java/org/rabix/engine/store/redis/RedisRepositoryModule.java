package org.rabix.engine.store.redis;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import org.apache.commons.configuration.Configuration;
import org.rabix.engine.store.redis.impl.*;
import org.rabix.engine.store.repository.*;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;

public class RedisRepositoryModule extends AbstractModule {

    private Configuration configuration;

    public RedisRepositoryModule(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    protected void configure() {
        bind(BackendRepository.class).to(RedisBackendRepository.class).in(Scopes.SINGLETON);
        bind(AppRepository.class).to(RedisAppRepository.class).in(Scopes.SINGLETON);
        bind(ContextRecordRepository.class).to(RedisContextRecordRepository.class).in(Scopes.SINGLETON);
        bind(DAGRepository.class).to(RedisDAGRepository.class).in(Scopes.SINGLETON);
        bind(EventRepository.class).to(RedisEventRepository.class).in(Scopes.SINGLETON);
        bind(IntermediaryFilesRepository.class).to(RedisIntermediaryFilesRepository.class).in(Scopes.SINGLETON);
        bind(JobRecordRepository.class).to(RedisJobRecordRepository.class).in(Scopes.SINGLETON);
        bind(JobRepository.class).to(RedisJobRepository.class).in(Scopes.SINGLETON);
        bind(JobStatsRecordRepository.class).to(RedisJobStatsRecordRepository.class).in(Scopes.SINGLETON);
        bind(LinkRecordRepository.class).to(RedisLinkRecordRepository.class).in(Scopes.SINGLETON);
        bind(VariableRecordRepository.class).to(RedisVariableRecordRepository.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    public RedisRepository getRedisRepository() {
        String host = configuration.getString("rabix.redis.host", "localhost");
        int port = configuration.getInt("rabix.redis.port", 6379);
        String password = configuration.getString("rabix.redis.password", null);
        boolean ssl = configuration.getBoolean("rabix.redis.ssl", false);
        int timeout = configuration.getInt("rabix.redis.timeout", 60);

        return new RedisRepositoryImpl(new JedisPool(buildPoolConfig(), host, port, timeout, password, ssl));
    }

    private JedisPoolConfig buildPoolConfig() {
        final JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(128);
        poolConfig.setMaxIdle(128);
        poolConfig.setMinIdle(16);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setMinEvictableIdleTimeMillis(Duration.ofSeconds(60).toMillis());
        poolConfig.setTimeBetweenEvictionRunsMillis(Duration.ofSeconds(30).toMillis());
        poolConfig.setNumTestsPerEvictionRun(3);
        poolConfig.setBlockWhenExhausted(true);
        return poolConfig;
    }
}
