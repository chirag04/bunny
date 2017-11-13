package org.rabix.engine.store.redis.impl;

import org.rabix.engine.store.redis.RedisRepository;
import org.rabix.engine.store.repository.IntermediaryFilesRepository;

import javax.inject.Inject;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class RedisIntermediaryFilesRepository implements IntermediaryFilesRepository  {

    private static final String INTERMEDIARY_FILES_NAMESPACE = "intermediary_files";

    private final RedisRepository redisRepository;

    @Inject
    public RedisIntermediaryFilesRepository(RedisRepository redisRepository) {
        this.redisRepository = redisRepository;
    }

    @Override
    public void insert(UUID rootId, String filename, Integer count) {
        update(rootId, filename, count);
    }

    @Override
    public void update(UUID rootId, String filename, Integer count) {
        redisRepository.set(inNamespace(rootId), filename, new IntermediaryFileEntity(rootId, filename, count));
    }

    @Override
    public void delete(UUID rootId, String filename) {
        redisRepository.delete(inNamespace(rootId), filename);
    }

    @Override
    public void delete(UUID rootId) {
        redisRepository.deleteAll(inNamespace(rootId));
    }

    @Override
    public void deleteByRootIds(Set<UUID> rootIds) {
        rootIds.forEach(this::delete);
    }

    @Override
    public List<IntermediaryFileEntity> get(UUID rootId) {
        return redisRepository.getAll(inNamespace(rootId), IntermediaryFileEntity.class);
    }

    @Override
    public IntermediaryFileEntity get(UUID rootId, String filename) {
        return redisRepository.get(inNamespace(rootId), filename, IntermediaryFileEntity.class);
    }

    private String inNamespace(UUID rootId) {
        return INTERMEDIARY_FILES_NAMESPACE + ":" + rootId;
    }
}
