package org.rabix.engine.service;

import java.util.Set;

import org.rabix.bindings.model.Job;

public interface Scheduler {
  public void scheduleJobs(Set<Job> jobs);
}
