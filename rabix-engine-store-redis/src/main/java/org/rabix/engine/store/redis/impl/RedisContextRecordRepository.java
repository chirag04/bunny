package org.rabix.engine.store.redis.impl;

import org.rabix.engine.store.model.ContextRecord;
import org.rabix.engine.store.redis.RedisRepository;
import org.rabix.engine.store.repository.ContextRecordRepository;

import javax.inject.Inject;
import java.util.UUID;

public class RedisContextRecordRepository implements ContextRecordRepository {

    private static final String CONTEXT_RECORD_NAMESPACE = "context";

    private final RedisRepository redisRepository;

    @Inject
    public RedisContextRecordRepository(RedisRepository redisRepository) {
        this.redisRepository = redisRepository;
    }

    @Override
    public int insert(ContextRecord contextRecord) {
        redisRepository.set(CONTEXT_RECORD_NAMESPACE, contextRecord.getId().toString(), contextRecord);
        return 0;
    }

    @Override
    public int update(ContextRecord contextRecord) {
        redisRepository.set(CONTEXT_RECORD_NAMESPACE, contextRecord.getId().toString(), contextRecord);
        return 0;
    }

    @Override
    public ContextRecord get(UUID id) {
        return redisRepository.get(CONTEXT_RECORD_NAMESPACE, id.toString(), ContextRecord.class);
    }

    @Override
    public int delete(UUID id) {
        redisRepository.delete(CONTEXT_RECORD_NAMESPACE, id.toString());
        return 0;
    }
}
