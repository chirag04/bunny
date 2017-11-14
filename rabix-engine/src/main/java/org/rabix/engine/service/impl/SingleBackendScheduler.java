package org.rabix.engine.service.impl;

import java.util.Set;

import org.rabix.bindings.model.Job;
import org.rabix.engine.service.Scheduler;
import org.rabix.engine.stub.BackendStub;

import com.google.inject.Inject;
import com.google.inject.name.Named;

public class SingleBackendScheduler implements Scheduler {

  private BackendStub backend;

  @Inject
  public SingleBackendScheduler(@Named("allBackends") Set<BackendStub> backends) {
    backend = backends.iterator().next();
  }

  @Override
  public void scheduleJobs(Set<Job> jobs) {
    jobs.forEach(job -> backend.send(job));
  }
}
