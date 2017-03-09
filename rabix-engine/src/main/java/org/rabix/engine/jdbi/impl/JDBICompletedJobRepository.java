package org.rabix.engine.jdbi.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import org.rabix.bindings.model.Job;
import org.rabix.bindings.model.Resources;
import org.rabix.common.helper.JSONHelper;
import org.rabix.engine.jdbi.impl.JDBICompletedJobRepository.CompletedJobEntityMapper;
import org.rabix.engine.jdbi.impl.JDBIJobRepository.BindJob;
import org.rabix.engine.repository.CompletedJobRepository;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterMapper;
import org.skife.jdbi.v2.sqlobject.stringtemplate.UseStringTemplate3StatementLocator;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

@RegisterMapper({CompletedJobEntityMapper.class })
@UseStringTemplate3StatementLocator
public interface JDBICompletedJobRepository extends CompletedJobRepository {

  @Override
  @SqlUpdate("insert into completed_job (id,root_id,name, parent_id, status, message, inputs, outputs, resources, backend_id, app, config) values (:id,:root_id,:name,:parent_id,:status::job_status,:message,:inputs::jsonb,:outputs::jsonb,:resources::jsonb,:backend_id,:app,:config::jsonb)")
  void insert(@BindJob Job job, @Bind("backend_id") UUID backendId);
  
  @Override
  @SqlUpdate("insert into completed_job (id,root_id,name, parent_id, status, message, inputs, outputs, resources, app, config) values (:id,:root_id,:name,:parent_id,:status::job_status,:message,:inputs::jsonb,:outputs::jsonb,:resources::jsonb,:app,:config::jsonb)")
  void insert(@BindJob Job job);

  
  public static class CompletedJobEntityMapper implements ResultSetMapper<CompletedJobEntity> {
    public CompletedJobEntity map(int index, ResultSet r, StatementContext ctx) throws SQLException {
      UUID id = r.getObject("id", UUID.class);
      UUID root_id = r.getObject("root_id", UUID.class);
      UUID backendId = r.getObject("backend_id", UUID.class);
      UUID parentId = r.getObject("parent_id", UUID.class);
      String name = r.getString("name");
      String app = r.getString("app");
      Job.JobStatus status = Job.JobStatus.valueOf(r.getString("status"));
      String message = r.getString("message");
      String inputsJson = r.getString("inputs");
      String outputsJson = r.getString("outputs");
      String configJson = r.getString("config");
      String resourcesStr = r.getString("resources");
      Resources res = JSONHelper.readObject(resourcesStr, Resources.class);
      

      Map<String, Object> inputs = JSONHelper.readMap(inputsJson);
      Map<String, Object> outputs = JSONHelper.readMap(outputsJson);
      Map<String, Object> config = JSONHelper.readMap(configJson);

      Job job = new Job(id, parentId, root_id, name, app, status, message, inputs, outputs, config, res, Collections.emptySet());
      return new CompletedJobEntity(job, backendId);
    }
  }  
}