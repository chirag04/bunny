package org.rabix.engine.store.redis.impl;

import com.beust.jcommander.internal.Maps;
import org.rabix.engine.store.model.BackendRecord;
import org.rabix.engine.store.redis.RedisRepositoryTest;
import org.rabix.engine.store.repository.BackendRepository;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.rabix.engine.store.model.BackendRecord.Status.ACTIVE;
import static org.rabix.engine.store.model.BackendRecord.Status.INACTIVE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class RedisBackendRepositoryTest extends RedisRepositoryTest {

    private BackendRepository backendRepository;

    @BeforeClass
    public void setUp() throws IOException {
        super.setUp();
        backendRepository = getInstance(BackendRepository.class);
    }

    @Test
    public void testInsertAndGet() throws Exception {
        BackendRecord backendRecord = insertRandom();
        BackendRecord retrievedRecord = backendRepository.get(backendRecord.getId());
        assertEquals(backendRecord, retrievedRecord);
    }

    @Test
    public void testGetByStatus() throws Exception {
        BackendRecord activeBackendRecord1 = insertRandom(ACTIVE);
        BackendRecord activeBackendRecord2 = insertRandom(ACTIVE);

        BackendRecord inactiveBackendRecord = insertRandom(INACTIVE);

        List<BackendRecord> activeBackendRecords = backendRepository.getByStatus(ACTIVE);
        assertEquals(activeBackendRecords.size(), 2);
        assertTrue(activeBackendRecords.contains(activeBackendRecord1) && activeBackendRecords.contains(activeBackendRecord2));

        List<BackendRecord> inactiveBackendRecords = backendRepository.getByStatus(INACTIVE);
        assertEquals(inactiveBackendRecords.size(), 1);
        assertEquals(inactiveBackendRecord, inactiveBackendRecords.get(0));
    }

    @Test
    public void testGetAll() throws Exception {
        List<BackendRecord> inserted = IntStream.range(0, 10).mapToObj(i -> insertRandom()).collect(Collectors.toList());
        List<BackendRecord> all = backendRepository.getAll();

        assertEquals(inserted.size(), all.size());
        assertTrue(all.containsAll(inserted));
    }

    @Test
    public void testUpdateHeartbeatInfo() throws Exception {
        BackendRecord backendRecord = insertRandom();

        Instant heartbeatInfo = Instant.now();
        backendRepository.updateHeartbeatInfo(backendRecord.getId(), heartbeatInfo);

        assertEquals(heartbeatInfo, backendRepository.get(backendRecord.getId()).getHeartbeatInfo());
    }

    @Test
    public void testUpdateStatus() throws Exception {
        BackendRecord backendRecord = insertRandom(ACTIVE);
        backendRepository.updateStatus(backendRecord.getId(), INACTIVE);

        assertEquals(INACTIVE, backendRepository.get(backendRecord.getId()).getStatus());
    }

    @Test
    public void testUpdateConfiguration() throws Exception {
        BackendRecord backendRecord = insertRandom(ACTIVE);

        Map<String, Object> config = new HashMap<>();
        config.put("diesel_id", 1234);

        backendRepository.updateConfiguration(backendRecord.getId(), config);

        assertEquals(config, backendRepository.get(backendRecord.getId()).getBackendConfig());
    }

    @Test
    public void testGetHeartbeatInfo() throws Exception {
        BackendRecord backendRecord = insertRandom();
        assertEquals(backendRecord.getHeartbeatInfo(), backendRepository.getHeartbeatInfo(backendRecord.getId()));
    }

    private BackendRecord insertRandom() {
        return insertRandom(ACTIVE);
    }

    private BackendRecord insertRandom(BackendRecord.Status status) {
        BackendRecord backendRecord = new BackendRecord(
                UUID.randomUUID(),
                "backend",
                Instant.now(),
                Maps.newHashMap(),
                status,
                BackendRecord.Type.RABBIT_MQ);
        backendRepository.insert(backendRecord);

        return backendRecord;
    }

}