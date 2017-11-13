package org.rabix.engine.store.redis.impl;

import org.rabix.bindings.model.dag.DAGNode;
import org.rabix.engine.store.redis.RedisRepository;
import org.rabix.engine.store.repository.DAGRepository;

import javax.inject.Inject;
import java.util.UUID;

public class RedisDAGRepository implements DAGRepository {

    private static final String DAG_NAMESPACE = "dag";

    private final RedisRepository redisRepository;

    @Inject
    public RedisDAGRepository(RedisRepository redisRepository) {
        this.redisRepository = redisRepository;
    }

    @Override
    public void insert(UUID rootId, DAGNode dag) {
        redisRepository.set(DAG_NAMESPACE, rootId.toString(), dag);
    }

    @Override
    public DAGNode get(String id, UUID rootId) {
        return redisRepository.get(DAG_NAMESPACE, rootId.toString(), DAGNode.class);
    }
}
