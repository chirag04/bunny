package org.rabix.engine.store.redis.impl;

import org.rabix.engine.store.model.JobRecord;
import org.rabix.engine.store.model.LinkRecord;
import org.rabix.engine.store.redis.RedisRepositoryTest;
import org.rabix.engine.store.repository.LinkRecordRepository;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.rabix.bindings.model.dag.DAGLinkPort.LinkPortType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class RedisLinkRecordRepositoryTest extends RedisRepositoryTest {

    private LinkRecordRepository linkRecordRepository;

    @BeforeClass
    public void setUp() throws IOException {
        super.setUp();
        linkRecordRepository = getInstance(LinkRecordRepository.class);
    }

    @Test
    public void testInsertBatch() throws Exception {
        UUID rootId = UUID.randomUUID();
        String sourceJobId = UUID.randomUUID().toString();

        List<LinkRecord> records = IntStream.range(0, 10).mapToObj(i -> generateRandom(rootId, sourceJobId)).collect(Collectors.toList());
        linkRecordRepository.insertBatch(records.iterator());

        List<LinkRecord> insertedRecords = linkRecordRepository.getBySource(sourceJobId, rootId);

        assertEquals(records.size(), insertedRecords.size());
        assertTrue(insertedRecords.containsAll(records));
    }

    @Test
    public void testDelete() throws Exception {
        UUID rootId = UUID.randomUUID();
        String sourceId = UUID.randomUUID().toString();

        LinkRecord linkRecord = generateRandom(rootId, sourceId);

        linkRecordRepository.insert(linkRecord);
        linkRecordRepository.delete(new HashSet<>(Collections.singletonList(new JobRecord.JobIdRootIdPair(sourceId, rootId))));

        assertTrue(linkRecordRepository.getBySource(sourceId, rootId).isEmpty());
    }

    @Test
    public void testInsert() throws Exception {
        UUID rootId = UUID.randomUUID();
        String sourceId = UUID.randomUUID().toString();

        LinkRecord linkRecord = generateRandom(rootId, sourceId);

        linkRecordRepository.insert(linkRecord);
        List<LinkRecord> linkRecords = linkRecordRepository.getBySource(sourceId, rootId);

        assertEquals(linkRecords.size(), 1);
        assertEquals(linkRecords.get(0), linkRecord);
    }

    @Test
    public void testGetBySourceCount() throws Exception {
        UUID rootId = UUID.randomUUID();
        String sourceId = UUID.randomUUID().toString();

        LinkRecord linkRecord = generateRandom(rootId, sourceId);
        linkRecordRepository.insert(linkRecord);

        assertEquals(
                linkRecordRepository.getBySourceCount(linkRecord.getSourceJobId(), linkRecord.getSourceJobPort(), rootId),
                1);
    }

    @Test
    public void testGetBySourceAndPortId() throws Exception {
        UUID rootId = UUID.randomUUID();
        String sourceId = UUID.randomUUID().toString();

        LinkRecord linkRecord = generateRandom(rootId, sourceId);
        linkRecordRepository.insert(linkRecord);

        assertEquals(linkRecord, linkRecordRepository.getBySource(sourceId, linkRecord.getSourceJobPort(), rootId).get(0));
    }

    @Test
    public void testGetBySourceAndSourceType() throws Exception {
        UUID rootId = UUID.randomUUID();
        String sourceId = UUID.randomUUID().toString();

        LinkRecord linkRecord = generateRandom(rootId, sourceId);
        linkRecordRepository.insert(linkRecord);

        List<LinkRecord> records = linkRecordRepository.getBySourceAndSourceType(sourceId, linkRecord.getSourceVarType(), rootId);
        assertEquals(records.size(), 1);
        assertEquals(linkRecord, records.get(0));
    }

    @Test
    public void testGetBySourceAndDestinationType() throws Exception {
        UUID rootId = UUID.randomUUID();
        String sourceId = UUID.randomUUID().toString();

        LinkRecord linkRecord = generateRandom(rootId, sourceId);
        linkRecordRepository.insert(linkRecord);

        List<LinkRecord> records = linkRecordRepository.getBySourceAndDestinationType(sourceId, linkRecord.getSourceJobPort(), linkRecord.getDestinationVarType(), rootId);
        assertEquals(records.size(), 1);
        assertEquals(linkRecord, records.get(0));
    }

    @Test
    public void testGetBySourceAndSourceType1() throws Exception {
        UUID rootId = UUID.randomUUID();
        String sourceId = UUID.randomUUID().toString();

        LinkRecord linkRecord = generateRandom(rootId, sourceId);
        linkRecordRepository.insert(linkRecord);

        List<LinkRecord> records = linkRecordRepository.getBySourceAndSourceType(sourceId, linkRecord.getSourceJobPort(), linkRecord.getSourceVarType(), rootId);
        assertEquals(records.size(), 1);
        assertEquals(linkRecord, records.get(0));
    }

    private LinkRecord generateRandom(UUID rootId, String sourceJobId) {
        Random random = new Random();
        return generateRandom(
                rootId,
                sourceJobId,
                "source_port" + random.nextInt(100),
                UUID.randomUUID().toString(),
                "destination_port" + random.nextInt(100));
    }

    private LinkRecord generateRandom(UUID rootId, String sourceJobId, String sourcePortId, String destinationJobId, String destinationPortId) {
        return new LinkRecord(rootId,
                sourceJobId, sourcePortId, LinkPortType.OUTPUT, destinationJobId, destinationPortId, LinkPortType.INPUT, new Random().nextInt(100));
    }
}