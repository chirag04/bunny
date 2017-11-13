package org.rabix.engine.store.redis.impl;

import org.rabix.engine.store.model.JobStatsRecord;
import org.rabix.engine.store.redis.RedisRepositoryTest;
import org.rabix.engine.store.repository.JobStatsRecordRepository;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class RedisJobStatsRecordRepositoryTest extends RedisRepositoryTest {

    private JobStatsRecordRepository jobStatsRecordRepository;

    @BeforeClass
    public void setUp() throws IOException {
        super.setUp();
        jobStatsRecordRepository = getInstance(JobStatsRecordRepository.class);
    }

    @Test
    public void testInsertAndGet() throws Exception {
        JobStatsRecord jobStatsRecord = insertRandom();
        assertEquals(jobStatsRecord, jobStatsRecordRepository.get(jobStatsRecord.getRootId()));
    }

    @Test
    public void testDelete() throws Exception {
        JobStatsRecord jobStatsRecord = insertRandom();
        jobStatsRecordRepository.delete(jobStatsRecord.getRootId());

        assertNull(jobStatsRecordRepository.get(jobStatsRecord.getRootId()));
    }

    private JobStatsRecord insertRandom() {
        Random random = new Random();
        JobStatsRecord jobStatsRecord = new JobStatsRecord(UUID.randomUUID(), random.nextInt(100), random.nextInt(100), random.nextInt(100));
        jobStatsRecordRepository.insert(jobStatsRecord);

        return jobStatsRecord;
    }

}