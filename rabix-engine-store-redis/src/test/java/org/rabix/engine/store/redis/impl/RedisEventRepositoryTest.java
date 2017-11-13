package org.rabix.engine.store.redis.impl;

import com.beust.jcommander.internal.Maps;
import org.rabix.engine.store.model.EventRecord;
import org.rabix.engine.store.redis.RedisRepositoryTest;
import org.rabix.engine.store.repository.EventRepository;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class RedisEventRepositoryTest extends RedisRepositoryTest {

    private EventRepository eventRepository;

    @BeforeClass
    public void setUp() throws IOException {
        super.setUp();
        this.eventRepository = getInstance(EventRepository.class);
    }

    @Test
    public void testDeleteGroup() throws Exception {
        UUID groupId = UUID.randomUUID();
        insertRandom(groupId);
        insertRandom(groupId);

        eventRepository.deleteGroup(groupId);
        List<EventRecord> eventRecords = eventRepository.findUnprocessed();

        assertTrue(eventRecords.isEmpty());
    }

    @Test
    public void testFindUnprocessed() throws Exception {
        EventRecord eventRecord = insertRandom();

        List<EventRecord> unprocessed = eventRepository.findUnprocessed();
        assertEquals(unprocessed.size(), 1);
        assertEquals(eventRecord, unprocessed.get(0));
    }

    private EventRecord insertRandom() {
        return insertRandom(UUID.randomUUID());
    }

    private EventRecord insertRandom(UUID groupId) {
        EventRecord eventRecord = new EventRecord(groupId, EventRecord.Status.UNPROCESSED, Maps.newHashMap());
        eventRepository.insert(eventRecord);

        return eventRecord;
    }

}