package org.rabix.engine.store.model;

import java.io.Serializable;
import java.time.LocalDateTime;

public class TimestampedModel implements Serializable {
  private LocalDateTime createdAt;
  private LocalDateTime modifiedAt;

  public TimestampedModel(LocalDateTime createdAt, LocalDateTime modifiedAt) {
    this.createdAt = createdAt;
    this.modifiedAt = modifiedAt;
  }

  public LocalDateTime getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(LocalDateTime createdAt) {
    this.createdAt = createdAt;
  }

  public LocalDateTime getModifiedAt() {
    return modifiedAt;
  }

  public void setModifiedAt(LocalDateTime modifiedAt) {
    this.modifiedAt = modifiedAt;
  }
}
