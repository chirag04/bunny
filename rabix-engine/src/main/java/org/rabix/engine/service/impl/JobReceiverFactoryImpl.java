package org.rabix.engine.service.impl;

import javax.inject.Inject;

import org.rabix.bindings.model.Job;
import org.rabix.engine.service.JobService;
import org.rabix.engine.service.JobServiceException;
import org.rabix.transport.mechanism.TransportPlugin.ReceiverCallbackFactory;
import org.rabix.transport.mechanism.TransportPlugin.ReceiveCallback;
import org.rabix.transport.mechanism.TransportPluginException;

public class JobReceiverFactoryImpl implements ReceiverCallbackFactory<Job> {
  @Inject
  private JobService jobService;

  @Override
  public ReceiveCallback<Job> getReceiver() {
    return (entity -> {
      try {
        jobService.update(entity);
      } catch (JobServiceException e) {
        throw new TransportPluginException("Failed to update Job", e);
      }
    });
  }
}
