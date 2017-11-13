package org.rabix.engine.store.repository;

import java.util.List;
import java.util.UUID;

import org.rabix.bindings.model.dag.DAGLinkPort.LinkPortType;
import org.rabix.engine.store.model.LinkRecord;

public abstract class LinkRecordRepository {
    
  public abstract int insert(LinkRecord linkRecord);
  
  public abstract List<LinkRecord> getBySourceAndSourceType(String sourceJobId, LinkPortType sourceType, UUID rootId);
  
  public abstract List<LinkRecord> getBySourceAndDestinationType(String sourceJobId, String sourceJobPortId, LinkPortType destinationType, UUID rootId);

  public abstract List<LinkRecord> getBySourceAndSourceType(String jobId, String portId, LinkPortType varType, UUID rootId);
}
