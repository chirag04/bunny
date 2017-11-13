package org.rabix.engine.store.redis.impl;

import org.apache.commons.lang3.RandomStringUtils;
import org.rabix.engine.store.redis.RedisRepositoryTest;
import org.rabix.engine.store.repository.IntermediaryFilesRepository;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.rabix.engine.store.repository.IntermediaryFilesRepository.IntermediaryFileEntity;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class RedisIntermediaryFilesRepositoryTest extends RedisRepositoryTest {

    private IntermediaryFilesRepository intermediaryFilesRepository;

    @BeforeClass
    public void setUp() throws IOException {
        super.setUp();
        this.intermediaryFilesRepository = getInstance(IntermediaryFilesRepository.class);
    }

    @Test
    public void testInsertAndGet() throws Exception {
        UUID rootId = UUID.randomUUID();

        IntermediaryFileEntity intermediaryFileEntity1 = insertRandomIntermediaryFileEntity(rootId);
        IntermediaryFileEntity intermediaryFileEntity2 = insertRandomIntermediaryFileEntity(rootId);
        IntermediaryFileEntity intermediaryFileEntity3 = insertRandomIntermediaryFileEntity(rootId);

        List<IntermediaryFileEntity> intermediaryFileEntities = intermediaryFilesRepository.get(rootId);

        assertEquals(intermediaryFileEntities.size(), 3);
        assertTrue(intermediaryFileEntities.containsAll(Arrays.asList(intermediaryFileEntity1, intermediaryFileEntity2, intermediaryFileEntity3)));
    }

    @Test
    public void testUpdate() throws Exception {
        UUID rootId = UUID.randomUUID();
        intermediaryFilesRepository.insert(rootId, "file_name", 1);
        intermediaryFilesRepository.update(rootId, "file_name", 2);

        List<IntermediaryFileEntity> intermediaryFileEntities = intermediaryFilesRepository.get(rootId);
        assertEquals(intermediaryFileEntities.size(), 1);

        IntermediaryFileEntity intermediaryFileEntity = intermediaryFileEntities.get(0);

        assertEquals(intermediaryFileEntity.getRootId(), rootId);
        assertEquals(intermediaryFileEntity.getFilename(), "file_name");
        assertEquals(intermediaryFileEntity.getCount().intValue(), 2);
    }

    @Test
    public void testDeleteFile() throws Exception {
        UUID rootId = UUID.randomUUID();
        IntermediaryFileEntity intermediaryFileEntity = insertRandomIntermediaryFileEntity(rootId);

        intermediaryFilesRepository.delete(rootId, intermediaryFileEntity.getFilename());
        assertNull(intermediaryFilesRepository.get(rootId, intermediaryFileEntity.getFilename()));
    }

    @Test
    public void testDeleteForRoot() throws Exception {
        UUID rootId = UUID.randomUUID();
        insertRandomIntermediaryFileEntity(rootId);

        intermediaryFilesRepository.delete(rootId);
        assertTrue(intermediaryFilesRepository.get(rootId).isEmpty());
    }

    @Test
    public void testDeleteByRootIds() throws Exception {
        Set<UUID> rootIds = IntStream.range(0, 10).mapToObj(i -> UUID.randomUUID()).collect(Collectors.toSet());
        rootIds.forEach(this::insertRandomIntermediaryFileEntity);

        intermediaryFilesRepository.deleteByRootIds(rootIds);
        rootIds.forEach(rootId -> assertTrue(intermediaryFilesRepository.get(rootId).isEmpty()));
    }

    private IntermediaryFileEntity insertRandomIntermediaryFileEntity(UUID rootId) {
        IntermediaryFileEntity intermediaryFileEntity =
                new IntermediaryFileEntity(rootId, RandomStringUtils.randomAlphanumeric(8), new Random().nextInt(8));
        intermediaryFilesRepository.insert(rootId, intermediaryFileEntity.getFilename(), intermediaryFileEntity.getCount());

        return intermediaryFileEntity;
    }

}