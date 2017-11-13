package org.rabix.engine.store.redis.impl;

import org.apache.commons.lang.RandomStringUtils;
import org.rabix.common.helper.ChecksumHelper;
import org.rabix.engine.store.model.JobRecord;
import org.rabix.engine.store.model.JobRecord.JobIdRootIdPair;
import org.rabix.engine.store.redis.RedisRepositoryTest;
import org.rabix.engine.store.repository.JobRecordRepository;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.testng.Assert.*;

public class RedisJobRecordRepositoryTest extends RedisRepositoryTest {

    private JobRecordRepository jobRecordRepository;

    @BeforeClass
    public void setUp() throws IOException {
        super.setUp();
        this.jobRecordRepository = getInstance(JobRecordRepository.class);
    }

    @Test
    public void testInsertAndGet() throws Exception {
        UUID rootId = UUID.randomUUID();
        UUID uniqueId = UUID.randomUUID();

        JobRecord jobRecord = insertRandomJobRecord(rootId, uniqueId);
        assertEquals(jobRecord, jobRecordRepository.get(jobRecord.getId(), rootId));
    }

    @Test
    public void testUpdate() throws Exception {
        UUID rootId = UUID.randomUUID();
        UUID uniqueId = UUID.randomUUID();

        JobRecord jobRecord = insertRandomJobRecord(rootId, uniqueId);
        jobRecord.setState(JobRecord.JobState.COMPLETED);

        jobRecordRepository.update(jobRecord);
        assertEquals(JobRecord.JobState.COMPLETED, jobRecordRepository.get(jobRecord.getId(), rootId).getState());
    }

    @Test
    public void testInsertBatch() throws Exception {
        UUID rootId = UUID.randomUUID();
        List<JobRecord> randomRecords = IntStream.range(0, 10).mapToObj(i -> insertRandomJobRecord(rootId)).collect(Collectors.toList());
        jobRecordRepository.insertBatch(randomRecords.iterator());

        List<JobRecord> insertedRecords = jobRecordRepository.get(rootId);

        assertEquals(insertedRecords.size(), randomRecords.size());
        assertTrue(insertedRecords.containsAll(randomRecords));
    }

    @Test
    public void testUpdateBatch() throws Exception {
        UUID rootId = UUID.randomUUID();
        List<JobRecord> randomRecords = IntStream.range(0, 10).mapToObj(i -> insertRandomJobRecord(rootId)).collect(Collectors.toList());
        jobRecordRepository.insertBatch(randomRecords.iterator());

        randomRecords.forEach(jobRecord -> jobRecord.setState(JobRecord.JobState.COMPLETED));
        jobRecordRepository.updateBatch(randomRecords.iterator());

        List<JobRecord> insertedRecords = jobRecordRepository.get(rootId);

        assertEquals(insertedRecords.size(), randomRecords.size());
        assertTrue(insertedRecords.stream().allMatch(jobRecord -> jobRecord.getState() == JobRecord.JobState.COMPLETED));
    }

    @Test
    public void testDeleteByStatus() throws Exception {
        UUID rootId = UUID.randomUUID();
        insertRandomJobRecord(rootId);
        insertRandomJobRecord(rootId);

        jobRecordRepository.deleteByStatus(JobRecord.JobState.READY);
        assertTrue(jobRecordRepository.getReady(rootId).isEmpty());
    }

    @Test
    public void testDelete() throws Exception {
        JobRecord jobRecord = insertRandomJobRecord();
        Set<JobIdRootIdPair> pairs = new HashSet<>(Collections.singletonList(new JobIdRootIdPair(jobRecord.getId(), jobRecord.getRootId())));

        jobRecordRepository.delete(pairs);
        assertNull(jobRecordRepository.get(jobRecord.getId(), jobRecord.getRootId()));
    }

    @Test
    public void testGetRoot() throws Exception {
        UUID rootId = UUID.randomUUID();
        JobRecord rootRecord = insertRandomJobRecord(rootId, UUID.randomUUID(), rootId);

        assertEquals(rootRecord, jobRecordRepository.getRoot(rootId));
    }

    @Test
    public void testGetById() throws Exception {
        JobRecord jobRecord = insertRandomJobRecord();
        assertEquals(jobRecord, jobRecordRepository.get(jobRecord.getId(), jobRecord.getRootId()));
    }

    @Test
    public void testGetByParent() throws Exception {
        UUID rootId = UUID.randomUUID();
        UUID parentId = UUID.randomUUID();

        JobRecord jobRecord = insertRandomJobRecord(rootId, parentId);
        assertEquals(jobRecord, jobRecordRepository.getByParent(parentId, rootId).get(0));
    }

    @Test
    public void testGetReady() throws Exception {
        UUID rootId = UUID.randomUUID();

        List<JobRecord> readyJobs = IntStream.range(0, 10).mapToObj(i -> insertRandomJobRecord(rootId)).collect(Collectors.toList());
        List<JobRecord> readyJobs2 = jobRecordRepository.getReady(rootId);

        assertEquals(readyJobs.size(), readyJobs2.size());
        assertTrue(readyJobs2.containsAll(readyJobs));
    }

    @Test
    public void testUpdateStatus() throws Exception {
        UUID rootId = UUID.randomUUID();

        List<JobRecord> jobRecords = IntStream.range(0, 10).mapToObj(i -> insertRandomJobRecord(rootId)).collect(Collectors.toList());
        jobRecordRepository.updateStatus(rootId, JobRecord.JobState.COMPLETED, new HashSet<>(Collections.singletonList(JobRecord.JobState.READY)));

        List<JobRecord> readyJobs = jobRecordRepository.get(rootId, new HashSet<>(Collections.singletonList(JobRecord.JobState.READY)));
        assertTrue(readyJobs.isEmpty());

        List<JobRecord> completedJobs = jobRecordRepository.get(rootId, new HashSet<>(Collections.singletonList(JobRecord.JobState.COMPLETED)));
        assertEquals(completedJobs.size(), jobRecords.size());
        assertTrue(completedJobs.containsAll(jobRecords));
    }

    private JobRecord insertRandomJobRecord(UUID rootId) {
        return insertRandomJobRecord(rootId, UUID.randomUUID());
    }

    private JobRecord insertRandomJobRecord() {
        return insertRandomJobRecord(UUID.randomUUID(), UUID.randomUUID());
    }

    private JobRecord insertRandomJobRecord(UUID rootId, UUID parentId) {
        return insertRandomJobRecord(rootId, parentId, UUID.randomUUID());
    }

        private JobRecord insertRandomJobRecord(UUID rootId, UUID parentId, UUID uniqueId) {
        JobRecord jobRecord =
                new JobRecord(
                        rootId,
                        RandomStringUtils.random(8),
                        uniqueId,
                        parentId,
                        JobRecord.JobState.READY,
                        false, false, false, false,
                        ChecksumHelper.checksum(RandomStringUtils.random(8), ChecksumHelper.HashAlgorithm.SHA1));
        jobRecordRepository.insert(jobRecord);

        return jobRecord;
    }
}