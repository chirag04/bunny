package org.rabix.engine.store.model;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;

public class EventRecord implements Serializable {

  public enum Status {
    PROCESSED,
    UNPROCESSED,
    FAILED
  }

  private UUID id;
  private UUID groupId;
  private Status status;
  private Map<String, ?> event;

  public EventRecord(UUID groupId, Status status, Map<String, ?> event) {
    this(UUID.randomUUID(), groupId, status, event);
  }

  public EventRecord(UUID eventId, UUID groupId, Status status, Map<String, ?> event) {
    this.id = eventId;
    this.groupId = groupId;
    this.status = status;
    this.event = event;
  }

  public UUID getId() {
    return id;
  }

  public void setId(UUID id) {
    this.id = id;
  }

  public UUID getGroupId() {
    return groupId;
  }

  public void setGroupId(UUID groupId) {
    this.groupId = groupId;
  }

  public Status getStatus() {
    return status;
  }

  public void setStatus(Status status) {
    this.status = status;
  }

  public Map<String, ?> getEvent() {
    return event;
  }

  public void setEvent(Map<String, ?> event) {
    this.event = event;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }

    if (!(obj instanceof EventRecord)) {
      return false;
    }

    if (obj == this) {
      return true;
    }

    EventRecord other = (EventRecord) obj;
    return new EqualsBuilder()
            .append(id, other.id)
            .append(groupId, other.groupId)
            .append(status, other.status)
            .append(event, other.event)
            .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
            .append(id)
            .append(groupId)
            .append(status)
            .append(event)
            .hashCode();
  }
}
