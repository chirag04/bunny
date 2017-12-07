package org.rabix.engine.service;

import java.util.Set;
import java.util.UUID;

public interface IntermediaryFilesHandler {

 public void handleUnusedFiles(UUID rootId, Set<String> unusedFiles);

}
