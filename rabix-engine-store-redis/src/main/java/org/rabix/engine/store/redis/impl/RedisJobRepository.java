package org.rabix.engine.store.redis.impl;

import org.rabix.bindings.model.Job;
import org.rabix.engine.store.redis.RedisRepository;
import org.rabix.engine.store.repository.JobRepository;

import javax.inject.Inject;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

public class RedisJobRepository implements JobRepository {

    private static final String JOB_NAMESPACE = "job";
    private static final String JOB_MEMBERS_NAMESPACE = "job.members";
    private static final String JOB_READY_NAMESPACE = "job.ready";

    private final RedisRepository redisRepository;

    @Inject
    public RedisJobRepository(RedisRepository redisRepository) {
        this.redisRepository = redisRepository;
    }

    @Override
    public void insert(Job job, UUID groupId, String producedByNode) {
        JobEntity jobEntity = new JobEntity(job, groupId, producedByNode);
        String jobId = job.getId().toString();

        redisRepository.set(JOB_NAMESPACE, jobId, jobEntity);

        updateReadyJobs(jobEntity);
        updateMembers(jobEntity);
    }

    @Override
    public void update(Job job) {
        JobEntity jobEntity = redisRepository.get(JOB_NAMESPACE, job.getId().toString(), JobEntity.class);
        if (jobEntity == null) {
            return;
        }
        jobEntity.setJob(job);

        redisRepository.set(JOB_NAMESPACE, job.getId().toString(), jobEntity);
        updateReadyJobs(jobEntity);
    }

    @Override
    public void update(Iterator<Job> jobs) {
        jobs.forEachRemaining(this::update);
    }

    @Override
    public void updateStatus(UUID rootId, Job.JobStatus updateStatus, Set<Job.JobStatus> whereStatuses) {
        List<String> ids = redisRepository.getList(JOB_MEMBERS_NAMESPACE, rootId.toString(), String.class);
        ids.forEach(id -> {
            JobEntity jobEntity = redisRepository.get(JOB_NAMESPACE, id, JobEntity.class);
            if (whereStatuses.contains(jobEntity.getJob().getStatus())) {
                jobEntity.setJob(Job.cloneWithStatus(jobEntity.getJob(), updateStatus));

                redisRepository.set(JOB_NAMESPACE, id, jobEntity);
                updateReadyJobs(jobEntity);
            }
        });
    }

    @Override
    public void updateBackendId(UUID jobId, UUID backendId) {
        JobEntity jobEntity = redisRepository.get(JOB_NAMESPACE, jobId.toString(), JobEntity.class);
        jobEntity.setBackendId(backendId);

        redisRepository.set(JOB_NAMESPACE, jobId.toString(), jobEntity);
    }

    @Override
    public void updateBackendIds(Iterator<JobEntity> jobBackendPair) {
        jobBackendPair.forEachRemaining(jobEntity -> updateBackendId(jobEntity.getJob().getId(), jobEntity.getBackendId()));
    }

    @Override
    public void dealocateJobs(UUID backendId) {
        List<JobEntity> jobs = redisRepository.getAll(JOB_NAMESPACE, JobEntity.class);

        jobs.stream().filter(jobEntity -> jobEntity.getBackendId().equals(backendId)).forEach(job -> {
            job.setBackendId(null);
            redisRepository.set(JOB_NAMESPACE, job.getJob().getId().toString(), job);
        });
    }

    @Override
    public Job get(UUID id) {
        JobEntity jobEntity = redisRepository.get(JOB_NAMESPACE, id.toString(), JobEntity.class);
        if (jobEntity == null) {
            return null;
        }

        return jobEntity.getJob();
    }

    @Override
    public Set<Job> get() {
        return redisRepository
                .getAll(JOB_NAMESPACE, JobEntity.class).stream()
                .map(JobEntity::getJob)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<Job> getByRootId(UUID rootId) {
        List<String> jobIds = redisRepository.getList(JOB_MEMBERS_NAMESPACE, rootId.toString(), String.class);
        return jobIds
                .stream()
                .map(id -> get(UUID.fromString(id)))
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<Job> getRootJobsForDeletion(Job.JobStatus status, Timestamp olderThanTime) {
        // TODO: implement a workaround
        return Collections.emptySet();
    }

    @Override
    public Set<Job> get(UUID rootID, Set<Job.JobStatus> whereStatuses) {
        return getByRootId(rootID)
                .stream()
                .filter(job -> whereStatuses.contains(job.getStatus()))
                .collect(Collectors.toSet());
    }

    @Override
    public Set<UUID> getBackendsByRootId(UUID rootId) {
        List<String> ids = redisRepository.getList(JOB_MEMBERS_NAMESPACE, rootId.toString(), String.class);
        List<JobEntity> jobs = ids.stream().map(id -> redisRepository.get(JOB_NAMESPACE, id, JobEntity.class)).collect(Collectors.toList());

        return jobs.stream().map(JobEntity::getBackendId).collect(Collectors.toSet());
    }

    @Override
    public UUID getBackendId(UUID jobId) {
        JobEntity jobEntity = redisRepository.get(JOB_NAMESPACE, jobId.toString(), JobEntity.class);
        if (jobEntity == null) {
            return null;
        }
        return jobEntity.getBackendId();
    }

    @Override
    public Set<Job> getReadyJobsByGroupId(UUID rootId, UUID groupId) {
        return getReadyJobs(rootId)
                .stream()
                .filter(jobEntity -> jobEntity.getGroupId().equals(groupId))
                .map(JobEntity::getJob)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<JobEntity> getReadyFree() {
        return getReadyJobs(null).stream().filter(jobEntity -> jobEntity.getBackendId() == null).collect(Collectors.toSet());
    }

    @Override
    public Job.JobStatus getStatus(UUID id) {
        return get(id).getStatus();
    }

    @Override
    public void deleteByRootIds(Set<UUID> rootIds) {
        rootIds.forEach(rootId -> {
            redisRepository.delete(JOB_NAMESPACE, rootId.toString());

            List<String> childrenIds = redisRepository.getList(JOB_MEMBERS_NAMESPACE, rootId.toString(), String.class);
            childrenIds.forEach(childId -> {
                redisRepository.delete(JOB_NAMESPACE, childId);
            });

            redisRepository.deleteAll(JOB_READY_NAMESPACE, rootId.toString());
            redisRepository.deleteAll(JOB_MEMBERS_NAMESPACE, rootId.toString());
        });
    }

    private List<JobEntity> getReadyJobs(UUID rootId) {
        String root = rootId == null ? null : rootId.toString();

        List<String> readyIds = redisRepository.getList(JOB_READY_NAMESPACE, root, String.class);
        return readyIds.stream().map(id -> redisRepository.get(JOB_NAMESPACE, id, JobEntity.class)).collect(Collectors.toList());
    }

    private void updateReadyJobs(JobEntity jobEntity) {
        Job job = jobEntity.getJob();
        String id = job.getId().toString();

        if (job.getStatus() == Job.JobStatus.READY) {
            redisRepository.append(JOB_READY_NAMESPACE, job.getRootId().toString(), id);
        } else {
            redisRepository.removeFromList(JOB_READY_NAMESPACE, job.getRootId().toString(), jobEntity);
        }
    }

    private void updateMembers(JobEntity jobEntity) {
        Job job = jobEntity.getJob();

        if (job.getRootId() != job.getId()) {
            String id = job.getId().toString();
            redisRepository.append(JOB_MEMBERS_NAMESPACE, job.getRootId().toString(), id);
        }
    }
}

