package org.rabix.engine.store.redis.impl;

import org.rabix.bindings.model.LinkMerge;
import org.rabix.bindings.model.dag.DAGLinkPort;
import org.rabix.engine.store.model.VariableRecord;
import org.rabix.engine.store.redis.RedisRepositoryTest;
import org.rabix.engine.store.repository.VariableRecordRepository;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class RedisVariableRecordRepositoryTest extends RedisRepositoryTest {

    private VariableRecordRepository variableRecordRepository;

    @BeforeClass
    public void setUp() throws IOException {
        super.setUp();
        variableRecordRepository = getInstance(VariableRecordRepository.class);
    }

    @Test
    public void testInsertBatch() throws Exception {
        UUID rootId = UUID.randomUUID();
        String jobId = UUID.randomUUID().toString();
        DAGLinkPort.LinkPortType type = DAGLinkPort.LinkPortType.OUTPUT;

        List<VariableRecord> records = IntStream.range(0, 10).mapToObj(i -> generateRandom(rootId, jobId, type)).collect(Collectors.toList());
        variableRecordRepository.insertBatch(records.iterator());

        List<VariableRecord> insertedRecords = variableRecordRepository.getByType(jobId, type, rootId);

        assertEquals(insertedRecords.size(), records.size());
        assertTrue(insertedRecords.containsAll(records));
    }

    @Test
    public void testUpdateBatch() throws Exception {
        UUID rootId = UUID.randomUUID();
        String jobId = UUID.randomUUID().toString();
        DAGLinkPort.LinkPortType type = DAGLinkPort.LinkPortType.OUTPUT;

        List<VariableRecord> records = IntStream.range(0, 10).mapToObj(i -> generateRandom(rootId, jobId, type)).collect(Collectors.toList());
        variableRecordRepository.insertBatch(records.iterator());

        List<VariableRecord> insertedRecords = variableRecordRepository.getByType(jobId, type, rootId);
        insertedRecords.forEach(insertedRecord -> insertedRecord.setValue(Maps.newHashMap()));

        variableRecordRepository.updateBatch(insertedRecords.iterator());
        List<VariableRecord> updatedRecords = variableRecordRepository.getByType(jobId, type, rootId);

        assertEquals(insertedRecords.size(), updatedRecords.size());
        updatedRecords.forEach(variableRecord -> assertEquals(variableRecord.getValue(), Maps.newHashMap()));
    }

    @Test
    public void testInsertAndGet() throws Exception {
        UUID rootId = UUID.randomUUID();
        String jobId = UUID.randomUUID().toString();
        String portId = "port_id";

        VariableRecord variableRecord1 = generateRandom(rootId, jobId, portId, DAGLinkPort.LinkPortType.OUTPUT);
        VariableRecord variableRecord2 = generateRandom(rootId, jobId, portId, DAGLinkPort.LinkPortType.INPUT);

        variableRecordRepository.insert(variableRecord1);
        variableRecordRepository.insert(variableRecord2);

        assertEquals(variableRecord1, variableRecordRepository.get(jobId, portId, DAGLinkPort.LinkPortType.OUTPUT, rootId));
        assertEquals(variableRecord2, variableRecordRepository.get(jobId, portId, DAGLinkPort.LinkPortType.INPUT, rootId));
    }

    @Test
    public void testGetByType() throws Exception {
        UUID rootId = UUID.randomUUID();
        String jobId = UUID.randomUUID().toString();
        String portId = "port_id";

        VariableRecord variableRecord1 = generateRandom(rootId, jobId, portId, DAGLinkPort.LinkPortType.INPUT);
        VariableRecord variableRecord2 = generateRandom(rootId, jobId, portId, DAGLinkPort.LinkPortType.INPUT);
        VariableRecord variableRecord3 = generateRandom(rootId, jobId, portId, DAGLinkPort.LinkPortType.OUTPUT);

        variableRecordRepository.insert(variableRecord1);
        variableRecordRepository.insert(variableRecord2);
        variableRecordRepository.insert(variableRecord3);

        List<VariableRecord> insertedRecords = variableRecordRepository.getByType(jobId, DAGLinkPort.LinkPortType.INPUT, rootId);

        assertEquals(insertedRecords.size(), 2);
        assertTrue(insertedRecords.containsAll(Arrays.asList(variableRecord1, variableRecord2)));
    }

    @Test
    public void testGetByPort() throws Exception {
        UUID rootId = UUID.randomUUID();
        String jobId = UUID.randomUUID().toString();
        String portId = "port_id";

        VariableRecord variableRecord1 = generateRandom(rootId, jobId, portId, DAGLinkPort.LinkPortType.OUTPUT);
        VariableRecord variableRecord2 = generateRandom(rootId, jobId, portId, DAGLinkPort.LinkPortType.INPUT);
        VariableRecord variableRecord3 = generateRandom(rootId, jobId, DAGLinkPort.LinkPortType.INPUT);

        variableRecordRepository.insert(variableRecord1);
        variableRecordRepository.insert(variableRecord2);
        variableRecordRepository.insert(variableRecord3);

        List<VariableRecord> insertedRecords = variableRecordRepository.getByPort(jobId, portId, rootId);

        assertEquals(insertedRecords.size(), 2);
        assertTrue(insertedRecords.containsAll(Arrays.asList(variableRecord1, variableRecord2)));
    }

    private VariableRecord generateRandom(UUID rootId, String jobId, DAGLinkPort.LinkPortType type) {
        return generateRandom(rootId, jobId, UUID.randomUUID().toString(), type);
    }

    private VariableRecord generateRandom(UUID rootId, String jobId, String portId, DAGLinkPort.LinkPortType type) {
        return new VariableRecord(rootId, jobId, portId, type, Collections.emptyList(), LinkMerge.merge_flattened);
    }
}