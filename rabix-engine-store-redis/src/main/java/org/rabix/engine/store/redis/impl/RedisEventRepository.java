package org.rabix.engine.store.redis.impl;

import org.rabix.engine.store.model.EventRecord;
import org.rabix.engine.store.redis.RedisRepository;
import org.rabix.engine.store.repository.EventRepository;

import javax.inject.Inject;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class RedisEventRepository implements EventRepository {

    private static final String EVENT_NAMESPACE = "event";

    private final RedisRepository redisRepository;

    @Inject
    public RedisEventRepository(RedisRepository redisRepository) {
        this.redisRepository = redisRepository;
    }

    @Override
    public void insert(EventRecord event) {
        redisRepository.set(EVENT_NAMESPACE, inGroup(event.getGroupId(), event.getId()), event);
    }

    @Override
    public void deleteGroup(UUID groupId) {
        redisRepository.deleteAll(EVENT_NAMESPACE + ":" + groupId);
    }

    @Override
    public List<EventRecord> findUnprocessed() {
        return redisRepository
                .getAll(EVENT_NAMESPACE, EventRecord.class)
                .stream().filter(eventRecord -> eventRecord.getStatus() == EventRecord.Status.UNPROCESSED)
                .collect(Collectors.toList());
    }

    private String inGroup(UUID groupId, UUID eventId) {
        return groupId.toString() + ":" + eventId.toString();
    }
}
