package org.rabix.bindings.model.dag;

import org.rabix.bindings.model.LinkMerge;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

public class DAGLink implements Serializable {

  @JsonProperty("source")
  private final DAGLinkPort source;
  @JsonProperty("destination")
  private final DAGLinkPort destination;

  @JsonProperty("position")
  private final Integer position;
  @JsonProperty("linkMerge")
  private final LinkMerge linkMerge;

  @JsonCreator
  public DAGLink(@JsonProperty("source") DAGLinkPort source, @JsonProperty("destination") DAGLinkPort destination, @JsonProperty("linkMerge") LinkMerge linkMerge, @JsonProperty("position") Integer position) {
    this.source = source;
    this.position = position;
    this.linkMerge = linkMerge;
    this.destination = destination;
  }

  public LinkMerge getLinkMerge() {
    return linkMerge;
  }

  public Integer getPosition() {
    return position;
  }

  public DAGLinkPort getSource() {
    return source;
  }

  public DAGLinkPort getDestination() {
    return destination;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((destination == null) ? 0 : destination.hashCode());
    result = prime * result + ((source == null) ? 0 : source.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    DAGLink other = (DAGLink) obj;
    if (destination == null) {
      if (other.destination != null)
        return false;
    } else if (!destination.equals(other.destination))
      return false;
    if (source == null) {
      if (other.source != null)
        return false;
    } else if (!source.equals(other.source))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "DAGLink [source=" + source + ", destination=" + destination + ", position=" + position + "]";
  }

}
