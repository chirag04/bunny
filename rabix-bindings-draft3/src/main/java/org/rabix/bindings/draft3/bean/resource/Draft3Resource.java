package org.rabix.bindings.draft3.bean.resource;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import org.rabix.bindings.draft3.bean.resource.requirement.*;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "class", defaultImpl = Draft3Resource.class, visible = true)
@JsonSubTypes({ @Type(value = Draft3DockerResource.class, name = "DockerRequirement"),
    @Type(value = Draft3InlineJavascriptRequirement.class, name = "InlineJavascriptRequirement"),
    @Type(value = Draft3ShellCommandRequirement.class, name = "ShellCommandRequirement"),
    @Type(value = Draft3ResourceRequirement.class, name = "ResourceRequirement"),
    @Type(value = Draft3SchemaDefRequirement.class, name = "SchemaDefRequirement"),
    @Type(value = Draft3CreateFileRequirement.class, name = "CreateFileRequirement"),
    @Type(value = Draft3EnvVarRequirement.class, name = "EnvVarRequirement") })
@JsonInclude(Include.NON_NULL)
public class Draft3Resource  implements Serializable {

  @JsonProperty("class")
  protected String type;
  protected Map<String, Object> raw = new HashMap<>();

  public Draft3Resource() {
  }

  @SuppressWarnings("unchecked")
  @JsonIgnore
  public <T> T getValue(String key) {
    if (raw == null) {
      return null;
    }

    Object value = raw.get(key);
    return value != null ? (T) value : null;
  }

  @JsonAnySetter
  public void add(String key, Object value) {
    raw.put(key, value);
  }

  @JsonAnyGetter
  public Map<String, Object> getRaw() {
    return raw;
  }

  @JsonIgnore
  public Draft3ResourceType getTypeEnum() {
    return Draft3ResourceType.OTHER;
  }

  @JsonTypeId
  public String getType() {
    return type;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((raw == null) ? 0 : raw.hashCode());
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
    Draft3Resource other = (Draft3Resource) obj;
    if (raw == null) {
      if (other.raw != null)
        return false;
    } else if (!raw.equals(other.raw))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "Draft3Resource [type=" + type + ", raw=" + raw + "]";
  }

}
