package org.rabix.engine.service;

import java.util.UUID;

import org.rabix.bindings.model.Job;

public interface IntermediaryFilesService {

  void handleJobDeleted(Job job);

  void handleJobFailed(Job job);

  void increment(UUID rootId, Object o);

  void decrement(UUID rootId, Object o);

  void handleUnusedFiles(UUID rootId);
}

