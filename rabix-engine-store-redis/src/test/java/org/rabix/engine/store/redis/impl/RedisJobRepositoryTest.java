package org.rabix.engine.store.redis.impl;

import org.rabix.bindings.model.Job;
import org.rabix.bindings.model.Resources;
import org.rabix.engine.store.redis.RedisRepositoryTest;
import org.rabix.engine.store.repository.JobRepository;
import org.rabix.engine.store.repository.JobRepository.JobEntity;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class RedisJobRepositoryTest extends RedisRepositoryTest {

    private JobRepository jobRepository;

    @BeforeClass
    public void setUp() throws IOException {
        super.setUp();
        jobRepository = getInstance(JobRepository.class);
    }

    @Test
    public void testInsertAndGetById() throws Exception {
        Job job = randomJob();
        jobRepository.insert(job, UUID.randomUUID(), "some_node");
        assertEquals(job, jobRepository.get(job.getId()));
    }

    @Test
    public void testUpdate() throws Exception {
        Job job = randomJob();
        jobRepository.insert(job, UUID.randomUUID(), "some_node");

        job = Job.cloneWithStatus(job, Job.JobStatus.FAILED);
        jobRepository.update(job);

        assertEquals(job, jobRepository.get(job.getId()));
    }

    @Test
    public void testUpdateStatus() throws Exception {
        UUID rootId = UUID.randomUUID();
        List<Job> jobs = IntStream.range(0, 10).mapToObj(i -> randomJob(rootId)).collect(Collectors.toList());
        jobs.forEach(job -> jobRepository.insert(job, job.getRootId(), "some_node_produced_this"));

        jobRepository.updateStatus(rootId, Job.JobStatus.COMPLETED, new HashSet<>(Collections.singletonList(Job.JobStatus.READY)));
        Set<Job> updatedJobs = jobRepository.getByRootId(rootId);

        updatedJobs.forEach(updatedJob -> assertTrue(updatedJob.getStatus() == Job.JobStatus.COMPLETED));
    }

    @Test
    public void testUpdateBackendId() throws Exception {
        UUID rootId = UUID.randomUUID();
        Job job = randomJob(rootId);

        jobRepository.insert(job, UUID.randomUUID(), "something");

        UUID backendId = UUID.randomUUID();
        jobRepository.updateBackendId(job.getId(), backendId);

        assertEquals(backendId, jobRepository.getBackendId(job.getId()));
    }

    @Test
    public void testDealocateJobs() throws Exception {
        UUID rootId = UUID.randomUUID();
        Job job1 = randomJob(rootId);
        Job job2 = randomJob(rootId);

        jobRepository.insert(job1, UUID.randomUUID(), "whatever");
        jobRepository.insert(job2, UUID.randomUUID(), "whatever");

        UUID backendId = UUID.randomUUID();
        jobRepository.updateBackendId(job1.getId(), backendId);
        jobRepository.updateBackendId(job2.getId(), backendId);

        jobRepository.dealocateJobs(backendId);

        assertNull(jobRepository.getBackendId(job1.getId()));
        assertNull(jobRepository.getBackendId(job2.getId()));
    }

    @Test
    public void testGetByRootId() throws Exception {
        UUID rootId = UUID.randomUUID();
        Job job1 = randomJob(rootId);
        Job job2 = randomJob(rootId);
        Job job3 = randomJob();

        jobRepository.insert(job1, UUID.randomUUID(), "whatever");
        jobRepository.insert(job2, UUID.randomUUID(), "whatever");
        jobRepository.insert(job3, UUID.randomUUID(), "whatever");

        Set<Job> jobs = jobRepository.getByRootId(rootId);

        assertEquals(jobs.size(), 2);
        assertTrue(jobs.containsAll(Arrays.asList(job1, job2)));
    }

    @Test
    public void testGetByStatuses() throws Exception {
        UUID rootId = UUID.randomUUID();
        Job job1 = randomJob(rootId);
        Job job2 = randomJob(rootId);
        Job job3 = randomJob();

        job3 = Job.cloneWithStatus(job3, Job.JobStatus.STARTED);

        jobRepository.insert(job1, UUID.randomUUID(), "whatever");
        jobRepository.insert(job2, UUID.randomUUID(), "whatever");
        jobRepository.insert(job3, UUID.randomUUID(), "whatever");

        Set<Job> pendingJobs = jobRepository.get(rootId, new HashSet<>(Collections.singletonList(Job.JobStatus.READY)));

        assertEquals(pendingJobs.size(), 2);
        pendingJobs.forEach(job -> assertTrue(job.getStatus() == Job.JobStatus.READY));
    }

    @Test
    public void testGetBackendsByRootId() throws Exception {
        UUID rootId = UUID.randomUUID();
        Job job1 = randomJob(rootId);
        Job job2 = randomJob(rootId);

        jobRepository.insert(job1, UUID.randomUUID(), "whatever");
        jobRepository.insert(job2, UUID.randomUUID(), "whatever");

        UUID backendId1 = UUID.randomUUID();
        UUID backendId2 = UUID.randomUUID();

        jobRepository.updateBackendId(job1.getId(), backendId1);
        jobRepository.updateBackendId(job2.getId(), backendId2);

        Set<UUID> backendIds = jobRepository.getBackendsByRootId(rootId);

        assertEquals(backendIds.size(), 2);
        assertTrue(backendIds.containsAll(Arrays.asList(backendId1, backendId2)));
    }

    @Test
    public void testGetBackendId() throws Exception {
        UUID rootId = UUID.randomUUID();
        Job job = randomJob(rootId);

        jobRepository.insert(job, UUID.randomUUID(), "whatever");

        UUID backendId = UUID.randomUUID();
        jobRepository.updateBackendId(job.getId(), backendId);

        UUID savedBackendId = jobRepository.getBackendId(job.getId());
        assertEquals(savedBackendId, backendId);
    }

    @Test
    public void testGetReadyJobsByGroupId() throws Exception {
        UUID rootId = UUID.randomUUID();
        Job job1 = randomJob(rootId);
        Job job2 = randomJob(rootId);

        UUID groupId = UUID.randomUUID();
        jobRepository.insert(job1, groupId, "whatever");
        jobRepository.insert(job2, groupId, "whatever");

        Set<Job> readyJobs = jobRepository.getReadyJobsByGroupId(rootId, groupId);

        assertEquals(readyJobs.size(), 2);
        readyJobs.forEach(job -> assertEquals(job.getStatus(), Job.JobStatus.READY));
    }

    @Test
    public void testGetReadyFree() throws Exception {
        UUID rootId = UUID.randomUUID();
        Job job1 = randomJob(rootId);
        Job job2 = randomJob(rootId);

        jobRepository.insert(job1, UUID.randomUUID(), "whatever");
        jobRepository.insert(job2, UUID.randomUUID(), "whatever");

        Set<JobEntity> readyFreeJobs = jobRepository.getReadyFree();
        assertEquals(readyFreeJobs.size(), 2);
        assertTrue(readyFreeJobs.stream().map(JobEntity::getJob).collect(Collectors.toSet()).containsAll(Arrays.asList(job1, job2)));
    }

    @Test
    public void testGetStatus() throws Exception {
        UUID rootId = UUID.randomUUID();
        Job job = randomJob(rootId);
        jobRepository.insert(job, UUID.randomUUID(), "whatever");

        assertEquals(jobRepository.getStatus(job.getId()), Job.JobStatus.READY);
    }

    @Test
    public void testDeleteByRootIds() throws Exception {
        UUID rootId = UUID.randomUUID();
        Job job1 = randomJob(rootId);
        Job job2 = randomJob(rootId);

        jobRepository.insert(job1, UUID.randomUUID(), "whatever");
        jobRepository.insert(job2, UUID.randomUUID(), "whatever");

        jobRepository.deleteByRootIds(new HashSet<>(Collections.singletonList(rootId)));

        assertTrue(jobRepository.getByRootId(rootId).isEmpty());
    }

    private Job randomJob(UUID rootId) {
       return new Job(
                UUID.randomUUID(),
                UUID.randomUUID(),
                rootId,
                "job" + new Random().nextInt(1000),
                "app",
                Job.JobStatus.READY,
                "message",
                Maps.newHashMap(),
                Maps.newHashMap(),
                Maps.newHashMap(),
                new Resources(0L, 0L, 0L, true, null, null, 0L, 0L),
                Collections.emptySet());
    }

    private Job randomJob() {
        return randomJob(UUID.randomUUID());
    }
}