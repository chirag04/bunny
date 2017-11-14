package org.rabix.engine.store.redis.impl;

import org.rabix.bindings.model.dag.DAGLinkPort.LinkPortType;
import org.rabix.engine.store.model.JobRecord;
import org.rabix.engine.store.model.VariableRecord;
import org.rabix.engine.store.redis.RedisRepository;
import org.rabix.engine.store.repository.VariableRecordRepository;

import javax.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;

public class RedisVariableRecordRepository extends VariableRecordRepository {

    private static final String VARIABLE_RECORD_NAMESPACE = "var_record";

    private final RedisRepository redisRepository;

    @Inject
    public RedisVariableRecordRepository(RedisRepository redisRepository) {
        this.redisRepository = redisRepository;
    }

    @Override
    public void insertBatch(Iterator<VariableRecord> records) {
        records.forEachRemaining(this::insert);
    }

    @Override
    public void updateBatch(Iterator<VariableRecord> records) {
        records.forEachRemaining(this::update);
    }

    @Override
    public int insert(VariableRecord variableRecord) {
        return update(variableRecord);
    }

    @Override
    public int update(VariableRecord variableRecord) {
        redisRepository.set(
                inNamespace(variableRecord.getRootId(), variableRecord.getJobId(), variableRecord.getType()),
                variableRecord.getPortId(),
                variableRecord);
        return 0;
    }

    @Override
    public void delete(Set<JobRecord.JobIdRootIdPair> externalIDs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public VariableRecord get(String jobId, String portId, LinkPortType type, UUID rootId) {
        Optional<VariableRecord> variableRecordOptional = getByPort(jobId, portId, rootId)
                .stream()
                .filter(variableRecord -> variableRecord.getType() == type)
                .findFirst();
        return variableRecordOptional.orElse(null);
    }

    @Override
    public List<VariableRecord> getByType(String jobId, LinkPortType type, UUID rootId) {
        return redisRepository
                .getAll(inNamespace(rootId, jobId, type), VariableRecord.class)
                .stream()
                .filter(variableRecord -> variableRecord.getType() == type)
                .collect(Collectors.toList());
    }

    @Override
    public List<VariableRecord> getByPort(String jobId, String portId, UUID rootId) {
        List<VariableRecord> variableRecords = new ArrayList<>();

        for (LinkPortType type : LinkPortType.values()) {
            variableRecords.addAll(getByType(jobId, type, rootId));
        }

        return variableRecords
                .stream()
                .filter(variableRecord -> variableRecord.getPortId().equals(portId))
                .collect(Collectors.toList());
    }

    private String inNamespace(UUID rootId, String jobId, LinkPortType type) {
        return VARIABLE_RECORD_NAMESPACE + ":" + rootId + ":" + jobId + ":" + (type == null ? "" : type);
    }
}
