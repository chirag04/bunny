package org.rabix.engine.store.redis.impl;

import org.rabix.engine.store.model.JobRecord;
import org.rabix.engine.store.redis.RedisRepository;
import org.rabix.engine.store.repository.JobRecordRepository;

import javax.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;

public class RedisJobRecordRepository extends JobRecordRepository {

    private static final String JOB_RECORD_NAMESPACE = "job_record";

    private final RedisRepository redisRepository;

    @Inject
    public RedisJobRecordRepository(RedisRepository redisRepository) {
        this.redisRepository = redisRepository;
    }

    @Override
    public int insert(JobRecord jobRecord) {
        return update(jobRecord);
    }

    @Override
    public int update(JobRecord jobRecord) {
        redisRepository.set(inNamespace(jobRecord.getRootId()), jobRecord.isRoot() ? null : jobRecord.getId(), jobRecord);
        return 0;
    }

    @Override
    public void insertBatch(Iterator<JobRecord> records) {
        records.forEachRemaining(this::insert);
    }

    @Override
    public void updateBatch(Iterator<JobRecord> records) {
        records.forEachRemaining(this::update);
    }

    @Override
    public int deleteByStatus(JobRecord.JobState state) {
        redisRepository
                .getAll(JOB_RECORD_NAMESPACE, JobRecord.class)
                .stream()
                .filter(jobRecord -> jobRecord.getState() == state)
                .forEach(jobRecord -> redisRepository.delete(inNamespace(jobRecord.getRootId()), jobRecord.getId()));
        return 0;
    }

    @Override
    public void delete(Set<JobRecord.JobIdRootIdPair> externalIDs) {
        externalIDs.forEach(pair -> redisRepository.delete(inNamespace(pair.rootId), pair.id));
    }

    @Override
    public List<JobRecord> get(UUID rootId) {
        return redisRepository.getAll(inNamespace(rootId), JobRecord.class);
    }

    @Override
    public JobRecord getRoot(UUID rootId) {
        Optional<JobRecord> optionalRoot = redisRepository.getAll(inNamespace(rootId), JobRecord.class).stream().filter(JobRecord::isRoot).findFirst();
        return optionalRoot.orElse(null);
    }

    @Override
    public JobRecord get(String id, UUID rootId) {
        return redisRepository.get(inNamespace(rootId), id, JobRecord.class);
    }

    @Override
    public List<JobRecord> getByParent(UUID parentId, UUID rootId) {
        return redisRepository
                .getAll(inNamespace(rootId), JobRecord.class)
                .stream()
                .filter(jobRecord -> jobRecord.getParentId().equals(parentId))
                .collect(Collectors.toList());
    }

    @Override
    public List<JobRecord> getReady(UUID rootId) {
        return redisRepository
                .getAll(inNamespace(rootId), JobRecord.class)
                .stream()
                .filter(jobRecord -> jobRecord.getState() == JobRecord.JobState.READY)
                .collect(Collectors.toList());
    }

    @Override
    public void updateStatus(UUID rootId, JobRecord.JobState state, Set<JobRecord.JobState> whereStates) {
        redisRepository
                .getAll(inNamespace(rootId), JobRecord.class)
                .stream()
                .filter(jobRecord -> whereStates.contains(jobRecord.getState()))
                .forEach(jobRecord -> {
                    jobRecord.setState(state);
                    update(jobRecord);
                });
    }

    @Override
    public List<JobRecord> get(UUID rootId, Set<JobRecord.JobState> states) {
        return redisRepository
                .getAll(inNamespace(rootId), JobRecord.class)
                .stream()
                .filter(jobRecord -> states.contains(jobRecord.getState()))
                .collect(Collectors.toList());
    }

    private String inNamespace(UUID rootId) {
        return JOB_RECORD_NAMESPACE + ":" + rootId;
    }
}
