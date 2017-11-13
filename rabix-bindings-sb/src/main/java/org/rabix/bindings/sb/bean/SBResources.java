package org.rabix.bindings.sb.bean;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

@JsonInclude(Include.NON_NULL)
public class SBResources implements Serializable {

  @JsonProperty("high_io")
  private final boolean highIO;
  @JsonProperty("cpu")
  private final Long cpu;
  @JsonProperty("mem")
  private final Long memMB;

  @JsonCreator
  public SBResources(@JsonProperty("high_io") boolean highIO, @JsonProperty("cpu") Long cpu,
      @JsonProperty("mem") Long memMB) {
    this.highIO = highIO;
    this.cpu = cpu;
    this.memMB = memMB;
  }

  public boolean isHighIO() {
    return highIO;
  }

  public Long getCpu() {
    return cpu;
  }

  public Long getMemMB() {
    return memMB;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((cpu == null) ? 0 : cpu.hashCode());
    result = prime * result + (highIO ? 1231 : 1237);
    result = prime * result + ((memMB == null) ? 0 : memMB.hashCode());
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
    SBResources other = (SBResources) obj;
    if (cpu == null) {
      if (other.cpu != null)
        return false;
    } else if (!cpu.equals(other.cpu))
      return false;
    if (highIO != other.highIO)
      return false;
    if (memMB == null) {
      if (other.memMB != null)
        return false;
    } else if (!memMB.equals(other.memMB))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "Resources [highIO=" + highIO + ", cpu=" + cpu + ", memMB=" + memMB + "]";
  }

}
