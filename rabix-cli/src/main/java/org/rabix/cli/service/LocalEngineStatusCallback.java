package org.rabix.cli.service;

import java.util.Map;

import org.rabix.bindings.BindingException;
import org.rabix.bindings.BindingsFactory;
import org.rabix.bindings.model.Job;
import org.rabix.common.helper.JSONHelper;
import org.rabix.engine.status.EngineStatusCallbackException;
import org.rabix.engine.status.impl.DefaultEngineStatusCallback;

import com.fasterxml.jackson.core.JsonProcessingException;

public class LocalEngineStatusCallback extends DefaultEngineStatusCallback {

  @Override
  public void onJobRootCompleted(Job rootJob) throws EngineStatusCallbackException {
    try {
      Map<String, Object> outputs = (Map<String, Object>) BindingsFactory.create(rootJob).translateToSpecific(rootJob.getOutputs());
      System.out.println(JSONHelper.mapper.writerWithDefaultPrettyPrinter().writeValueAsString(outputs));
      System.exit(0);
    } catch (BindingException | JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
