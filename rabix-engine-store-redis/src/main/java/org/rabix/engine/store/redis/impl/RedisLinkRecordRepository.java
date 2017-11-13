package org.rabix.engine.store.redis.impl;

import org.rabix.bindings.model.dag.DAGLinkPort;
import org.rabix.engine.store.model.JobRecord;
import org.rabix.engine.store.model.LinkRecord;
import org.rabix.engine.store.redis.RedisRepository;
import org.rabix.engine.store.repository.LinkRecordRepository;

import javax.inject.Inject;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class RedisLinkRecordRepository extends LinkRecordRepository {

    private static final String LINK_RECORD_NAMESPACE = "link_record";

    private final RedisRepository redisRepository;

    @Inject
    public RedisLinkRecordRepository(RedisRepository redisRepository) {
        this.redisRepository = redisRepository;
    }

    @Override
    public void insertBatch(Iterator<LinkRecord> records) {
        updateBatch(records);
    }

    @Override
    public void updateBatch(Iterator<LinkRecord> records) {
        records.forEachRemaining(this::update);
    }

    @Override
    public void delete(Set<JobRecord.JobIdRootIdPair> pairs) {
        pairs.forEach(pair -> {
            List<LinkRecord> linkRecords = getBySource(pair.id, pair.rootId);
            linkRecords.forEach(linkRecord -> redisRepository.delete(inNamespace(pair.rootId), String.valueOf(linkRecord.hashCode())));
        });
    }

    @Override
    public int insert(LinkRecord linkRecord) {
        return update(linkRecord);
    }

    @Override
    public int update(LinkRecord linkRecord) {
        redisRepository.set(inNamespace(linkRecord.getRootId()), String.valueOf(linkRecord.hashCode()), linkRecord);
        return 0;
    }

    @Override
    public List<LinkRecord> getBySource(String sourceJobId, UUID rootId) {
        return redisRepository
                .getAll(inNamespace(rootId), LinkRecord.class)
                .stream()
                .filter(linkRecord -> linkRecord.getSourceJobId().equals(sourceJobId))
                .collect(Collectors.toList());
    }

    @Override
    public int getBySourceCount(String sourceJobId, String sourceJobPortId, UUID rootId) {
        return getBySource(sourceJobId, sourceJobPortId, rootId).size();
    }

    @Override
    public List<LinkRecord> getBySource(String sourceJobId, String sourceJobPortId, UUID rootId) {
        return getBySource(sourceJobId, rootId)
                .stream()
                .filter(linkRecord -> linkRecord.getSourceJobPort().equals(sourceJobPortId))
                .collect(Collectors.toList());
    }

    @Override
    public List<LinkRecord> getBySourceJobId(String sourceJobId, UUID rootId) {
        return getBySource(sourceJobId, rootId);
    }

    @Override
    public List<LinkRecord> getBySourceAndSourceType(String sourceJobId, DAGLinkPort.LinkPortType sourceType, UUID rootId) {
        return getBySource(sourceJobId, rootId)
                .stream()
                .filter(linkRecord -> linkRecord.getSourceVarType().equals(sourceType))
                .collect(Collectors.toList());
    }

    @Override
    public List<LinkRecord> getBySourceAndDestinationType(String sourceJobId, String sourceJobPortId, DAGLinkPort.LinkPortType destinationType, UUID rootId) {
        return getBySource(sourceJobId, sourceJobPortId, rootId)
                .stream()
                .filter(linkRecord -> linkRecord.getDestinationVarType().equals(destinationType))
                .collect(Collectors.toList());
    }

    @Override
    public List<LinkRecord> getBySourceAndSourceType(String jobId, String portId, DAGLinkPort.LinkPortType varType, UUID rootId) {
        return getBySource(jobId, portId, rootId)
                .stream()
                .filter(linkRecord -> linkRecord.getSourceVarType().equals(varType))
                .collect(Collectors.toList());
    }

    private String inNamespace(UUID rootId) {
        return LINK_RECORD_NAMESPACE + ":" + rootId;
    }
}
