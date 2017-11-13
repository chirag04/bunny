package org.rabix.engine.store.redis.impl;

import com.beust.jcommander.internal.Maps;
import org.rabix.engine.store.model.ContextRecord;
import org.rabix.engine.store.redis.RedisRepositoryTest;
import org.rabix.engine.store.repository.ContextRecordRepository;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class RedisContextRecordRepositoryTest extends RedisRepositoryTest {

    private ContextRecordRepository contextRecordRepository;

    @BeforeClass
    public void setUp() throws IOException {
        super.setUp();
        contextRecordRepository = getInstance(ContextRecordRepository.class);
    }

    @Test
    public void testInsertAndGet() throws Exception {
        ContextRecord contextRecord = insertRandom();
        assertEquals(contextRecord, contextRecordRepository.get(contextRecord.getId()));
    }

    @Test
    public void testUpdate() throws Exception {
        ContextRecord contextRecord = insertRandom();
        contextRecord.setStatus(ContextRecord.ContextStatus.FAILED);

        contextRecordRepository.update(contextRecord);
        assertEquals(contextRecord, contextRecordRepository.get(contextRecord.getId()));
    }

    @Test
    public void testDelete() throws Exception {
        ContextRecord contextRecord = insertRandom();
        contextRecordRepository.insert(contextRecord);
        contextRecordRepository.delete(contextRecord.getId());

        assertNull(contextRecordRepository.get(contextRecord.getId()));
    }

    private ContextRecord insertRandom() {
        ContextRecord contextRecord = new ContextRecord(UUID.randomUUID(), Maps.newHashMap(), ContextRecord.ContextStatus.RUNNING);
        contextRecordRepository.insert(contextRecord);

        return contextRecord;
    }

}