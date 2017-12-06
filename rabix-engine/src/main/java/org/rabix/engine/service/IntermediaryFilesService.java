package org.rabix.engine.service;

import java.util.UUID;

import org.rabix.bindings.model.Job;

public interface IntermediaryFilesService {

  void handleUnusedFiles(Job job);

  void handleJobDeleted(Job job);

  void handleJobFailed(Job job);

  void handleObjectSent(UUID rootId, Object o);
}

