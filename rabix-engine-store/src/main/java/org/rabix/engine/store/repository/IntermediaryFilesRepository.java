package org.rabix.engine.store.repository;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public interface IntermediaryFilesRepository {

  void insert(UUID rootId, String filename, Integer count);

  void update(UUID rootId, String filename, Integer count);

  void delete(UUID rootId, String filename);

  void delete(UUID rootId);

  void deleteByRootIds(Set<UUID> rootIds);

  List<IntermediaryFileEntity> get(UUID rootId);

  IntermediaryFileEntity get(UUID rootId, String filename);

  class IntermediaryFileEntity implements Serializable {

    private UUID rootId;
    private String filename;
    private Integer count;

    public IntermediaryFileEntity(UUID rootId, String filename, Integer count) {
      this.rootId = rootId;
      this.filename = filename;
      this.count = count;
    }

    public UUID getRootId() {
      return rootId;
    }

    public void setRootId(UUID rootId) {
      this.rootId = rootId;
    }

    public String getFilename() {
      return filename;
    }

    public void setFilename(String filename) {
      this.filename = filename;
    }

    public Integer getCount() {
      return count;
    }

    public void setCount(Integer count) {
      this.count = count;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }

      if (!(obj instanceof IntermediaryFileEntity)) {
        return false;
      }

      if (obj == this) {
        return true;
      }

      IntermediaryFileEntity other = (IntermediaryFileEntity) obj;
      return new EqualsBuilder()
              .append(rootId, other.rootId)
              .append(filename, other.filename)
              .append(count, other.count)
              .isEquals();
    }

    @Override
    public int hashCode() {
      return new HashCodeBuilder()
              .append(rootId)
              .append(filename)
              .append(count)
              .hashCode();
    }

    @Override
    public String toString() {
      return "IntermediaryFileEntity [filename=" + filename + ", count=" + count + "]";
    }
  }

}
