package org.rabix.engine.store.redis.impl;

import org.rabix.engine.store.model.JobStatsRecord;
import org.rabix.engine.store.redis.RedisRepository;
import org.rabix.engine.store.repository.JobStatsRecordRepository;

import javax.inject.Inject;
import java.util.UUID;

public class RedisJobStatsRecordRepository implements JobStatsRecordRepository {

    private static final String JOB_STATS_RECORD_NAMESPACE = "job_stats_record";

    private final RedisRepository redisRepository;

    @Inject
    public RedisJobStatsRecordRepository(RedisRepository redisRepository) {
        this.redisRepository = redisRepository;
    }

    @Override
    public int insert(JobStatsRecord jobStatsRecord) {
        return update(jobStatsRecord);
    }

    @Override
    public int update(JobStatsRecord jobStatsRecord) {
        redisRepository.set(JOB_STATS_RECORD_NAMESPACE, jobStatsRecord.getRootId().toString(), jobStatsRecord);
        return 0;
    }

    @Override
    public JobStatsRecord get(UUID rootId) {
        return redisRepository.get(JOB_STATS_RECORD_NAMESPACE, rootId.toString(), JobStatsRecord.class);
    }

    @Override
    public int delete(UUID rootId) {
        redisRepository.delete(JOB_STATS_RECORD_NAMESPACE, rootId.toString());
        return 0;
    }
}
