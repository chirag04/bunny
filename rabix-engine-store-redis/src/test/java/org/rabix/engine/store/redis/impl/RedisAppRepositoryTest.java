package org.rabix.engine.store.redis.impl;

import org.apache.commons.codec.binary.Base64;
import org.rabix.common.helper.ChecksumHelper;
import org.rabix.engine.store.redis.RedisRepositoryTest;
import org.rabix.engine.store.repository.AppRepository;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Random;

import static org.testng.Assert.assertEquals;

public class RedisAppRepositoryTest extends RedisRepositoryTest {

    private AppRepository appRepository;

    @BeforeClass
    public void setUp() throws IOException {
        super.setUp();
        appRepository = getInstance(AppRepository.class);
    }

    @Test
    public void testInsertAndGet() throws Exception {
        byte[] randomBytes = new byte[4096];
        new Random().nextBytes(randomBytes);

        String app = Base64.encodeBase64String(randomBytes);
        String hash = ChecksumHelper.checksum(app, ChecksumHelper.HashAlgorithm.SHA1);

        appRepository.insert(hash, app);
        assertEquals(app, appRepository.get(hash));
    }
}