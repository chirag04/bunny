package org.rabix.engine.store.repository;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.rabix.bindings.model.dag.DAGLinkPort.LinkPortType;
import org.rabix.engine.store.model.JobRecord;
import org.rabix.engine.store.model.VariableRecord;

public abstract class VariableRecordRepository {
  
  public abstract int insert(VariableRecord jobRecord);
  
  public abstract int update(VariableRecord jobRecord);
  
  public abstract void delete(Set<JobRecord.JobIdRootIdPair> externalIDs);
  
  public abstract VariableRecord get(String jobId, String portId, LinkPortType type, UUID rootId);
 
  public abstract List<VariableRecord> getByType(String jobId, LinkPortType type, UUID rootId);
   
}