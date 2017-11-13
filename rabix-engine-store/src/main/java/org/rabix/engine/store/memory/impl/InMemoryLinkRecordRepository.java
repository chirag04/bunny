package org.rabix.engine.store.memory.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.rabix.bindings.model.dag.DAGLinkPort.LinkPortType;
import org.rabix.engine.store.model.LinkRecord;
import org.rabix.engine.store.repository.LinkRecordRepository;

import com.google.inject.Inject;

public class InMemoryLinkRecordRepository extends LinkRecordRepository {

  Map<UUID, List<LinkRecord>> linkRecordRepository;
  
  @Inject
  public InMemoryLinkRecordRepository() {
    this.linkRecordRepository = new ConcurrentHashMap<UUID, List<LinkRecord>>();
  }


  @Override
  public synchronized int insert(LinkRecord linkRecord) {
    insertLinkRecord(linkRecord);
    return 1;
  }

  @Override
  public synchronized List<LinkRecord> getBySourceAndSourceType(String sourceJobId, LinkPortType sourceType, UUID rootId) {
    List<LinkRecord> result = new ArrayList<>();
    for (LinkRecord lr : getLinkRecords(rootId)) {
      if (lr.getSourceJobId().equals(sourceJobId) && lr.getSourceVarType().equals(sourceType)) {
        result.add(lr);
      }
    }
    return result;
  }

  @Override
  public synchronized List<LinkRecord> getBySourceAndDestinationType(String sourceJobId, String sourceJobPortId, LinkPortType destinationType, UUID rootId) {
    List<LinkRecord> result = new ArrayList<>();
    for (LinkRecord lr : getLinkRecords(rootId)) {
      if (lr.getSourceJobId().equals(sourceJobId) && lr.getSourceJobPort().equals(sourceJobPortId) && lr.getDestinationVarType().equals(destinationType)) {
        result.add(lr);
      }
    }
    return result;
  }
  
  private synchronized void insertLinkRecord(LinkRecord linkRecord) {
    getLinkRecords(linkRecord.getRootId()).add(linkRecord);
  }
  
  private List<LinkRecord> getLinkRecords(UUID contextId) {
    List<LinkRecord> linkList = linkRecordRepository.get(contextId);
    if (linkList == null) {
      linkList = new ArrayList<>();
      linkRecordRepository.put(contextId, linkList);
    }
    return linkList;
  }

  @Override
  public List<LinkRecord> getBySourceAndSourceType(String jobId, String portId, LinkPortType varType, UUID rootId) {
    List<LinkRecord> result = new ArrayList<>();
    for (LinkRecord lr : getLinkRecords(rootId)) {
      if (lr.getSourceJobId().equals(jobId) && lr.getSourceJobPort().equals(portId) && lr.getSourceVarType().equals(varType)) {
        result.add(lr);
      }
    }
    return result;
  }
  
}
