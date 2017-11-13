package org.rabix.engine.store.redis.impl;

import org.rabix.engine.store.redis.RedisRepository;
import org.rabix.engine.store.repository.AppRepository;

import javax.inject.Inject;

public class RedisAppRepository implements AppRepository {

    private static final String APP_NAMESPACE = "app";

    private final RedisRepository redisRepository;

    @Inject
    public RedisAppRepository(RedisRepository redisRepository) {
        this.redisRepository = redisRepository;
    }

    @Override
    public void insert(String hash, String app) {
        redisRepository.set(APP_NAMESPACE, hash, app);
    }

    @Override
    public String get(String hash) {
        return redisRepository.get(APP_NAMESPACE, hash, String.class);
    }
}
