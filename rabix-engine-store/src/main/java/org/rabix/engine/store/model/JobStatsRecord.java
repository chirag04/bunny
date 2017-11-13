package org.rabix.engine.store.model;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;
import java.util.UUID;

public class JobStatsRecord implements Serializable {

  private UUID rootId;
  private int completed;
  private int running;
  private int total;

  public JobStatsRecord(final UUID rootId, int completed, int running, int total) {
    this.rootId = rootId;
    this.completed = completed;
    this.running = running;
    this.total = total;
  }

  public UUID getRootId() {
    return rootId;
  }

  public void setRootId(UUID rootId) {
    this.rootId = rootId;
  }

  public int getCompleted() {
    return completed;
  }

  public void setCompleted(int completed) {
    this.completed = completed;
  }

  public int getRunning() {
    return running;
  }

  public int getTotal() {
    return total;
  }

  public void setTotal(int total) {
    this.total = total;
  }

  public void setRunning(int running) {
    this.running = running;
  }
  public void increaseRunning() {
    running++;
  }
  public void increaseCompleted() {
    completed++;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }

    if (!(obj instanceof JobStatsRecord)) {
      return false;
    }

    if (obj == this) {
      return true;
    }

    JobStatsRecord other = (JobStatsRecord) obj;
    return new EqualsBuilder()
            .append(rootId, other.rootId)
            .append(completed, other.completed)
            .append(running, other.running)
            .append(total, other.total)
            .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
            .append(rootId)
            .append(completed)
            .append(running)
            .append(total)
            .hashCode();
  }

  @Override public String toString() {
    return "JobStatsRecord{" + "rootId=" + rootId + ", completed=" + completed + ", running=" + running + ", total="
        + total + '}';
  }
}
