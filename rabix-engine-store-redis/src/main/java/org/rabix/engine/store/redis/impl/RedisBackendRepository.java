package org.rabix.engine.store.redis.impl;

import org.rabix.engine.store.model.BackendRecord;
import org.rabix.engine.store.redis.RedisRepository;
import org.rabix.engine.store.repository.BackendRepository;

import javax.inject.Inject;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class RedisBackendRepository implements BackendRepository {

    private static final String BACKEND_RECORD_NAMESPACE = "backend";

    private final RedisRepository redisRepository;

    @Inject
    public RedisBackendRepository(RedisRepository redisRepository) {
        this.redisRepository = redisRepository;
    }

    @Override
    public void insert(BackendRecord backend) {
        redisRepository.set(BACKEND_RECORD_NAMESPACE, backend.getId().toString(), backend);
    }

    @Override
    public BackendRecord get(UUID id) {
        return redisRepository.get(BACKEND_RECORD_NAMESPACE, id.toString(), BackendRecord.class);
    }

    @Override
    public List<BackendRecord> getByStatus(BackendRecord.Status status) {
        return getAll()
                .stream()
                .filter(record -> record.getStatus() == status)
                .collect(Collectors.toList());
    }

    @Override
    public List<BackendRecord> getAll() {
        return redisRepository.getAll(BACKEND_RECORD_NAMESPACE, BackendRecord.class);
    }

    @Override
    public void updateHeartbeatInfo(UUID id, Instant heartbeatInfo) {
        BackendRecord record = redisRepository.get(BACKEND_RECORD_NAMESPACE, id.toString(), BackendRecord.class);
        record.setHeartbeatInfo(heartbeatInfo);

        redisRepository.set(BACKEND_RECORD_NAMESPACE, id.toString(), record);
    }

    @Override
    public void updateStatus(UUID id, BackendRecord.Status status) {
        BackendRecord record = redisRepository.get(BACKEND_RECORD_NAMESPACE, id.toString(), BackendRecord.class);
        record.setStatus(status);

        redisRepository.set(BACKEND_RECORD_NAMESPACE, id.toString(), record);
    }

    @Override
    public void updateConfiguration(UUID id, Map<String, ?> backendConfiguration) {
        BackendRecord record = redisRepository.get(BACKEND_RECORD_NAMESPACE, id.toString(), BackendRecord.class);
        record.setBackendConfig(backendConfiguration);

        redisRepository.set(BACKEND_RECORD_NAMESPACE, id.toString(), record);
    }

    @Override
    public Instant getHeartbeatInfo(UUID id) {
        BackendRecord record = redisRepository.get(BACKEND_RECORD_NAMESPACE, id.toString(), BackendRecord.class);
        return record.getHeartbeatInfo();
    }
}
