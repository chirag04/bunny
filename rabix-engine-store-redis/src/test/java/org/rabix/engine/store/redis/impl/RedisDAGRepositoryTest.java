package org.rabix.engine.store.redis.impl;

import com.beust.jcommander.internal.Maps;
import org.rabix.bindings.Bindings;
import org.rabix.bindings.BindingsFactory;
import org.rabix.bindings.model.Job;
import org.rabix.bindings.model.dag.DAGNode;
import org.rabix.common.helper.ResourceHelper;
import org.rabix.engine.store.redis.RedisRepositoryTest;
import org.rabix.engine.store.repository.DAGRepository;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.Assert.assertEquals;

public class RedisDAGRepositoryTest extends RedisRepositoryTest {

    private DAGRepository dagRepository;

    @BeforeClass
    public void setUp() throws IOException {
        super.setUp();
        dagRepository = getInstance(DAGRepository.class);
    }

    @Test
    public void testInsertAndGet() throws Exception {
        String app = ResourceHelper.readResource("joint-calling-per-sample-to-gvcfs");
        Job job = buildJob(app);

        Bindings bindings = BindingsFactory.create(job);
        DAGNode dagNode = bindings.translateToDAG(job);

        dagRepository.insert(job.getRootId(), dagNode);

        assertEquals(dagNode, dagRepository.get(dagNode.getId(), job.getRootId()));
    }

    private Job buildJob(String app) {
        return new Job(app, Maps.newHashMap());
    }
}