package org.rabix.engine.store.redis;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.commons.configuration.SystemConfiguration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import redis.embedded.RedisServer;

import java.io.IOException;

@Test(groups = "functional")
public class RedisRepositoryTest {

    private Injector injector;
    private RedisRepository redisRepository;
    private RedisServer redis;

    @BeforeClass
    public void setUp() throws IOException {
        redis = new RedisServer();
        redis.start();

        injector = Guice.createInjector(new RedisRepositoryModule(new SystemConfiguration()));
        redisRepository = injector.getInstance(RedisRepository.class);
    }

    @AfterClass
    public void cleanUp() {
        redis.stop();
    }

    @AfterMethod
    public void clear() {
        redisRepository.flushAll();
    }

    protected <T> T getInstance(Class<T> type) {
        return injector.getInstance(type);
    }
}
