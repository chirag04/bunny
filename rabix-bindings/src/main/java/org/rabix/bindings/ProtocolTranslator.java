package org.rabix.bindings;

import org.rabix.bindings.model.dag.DAGNode;

/**
 * Translates specific protocol to generic DAG format 
 */
public interface ProtocolTranslator {

  /**
   * Translates to DAG format
   */
  DAGNode translateToDAGFromPayload(String payload) throws BindingException;
  
  /**
   * Get inputs from the pay-load
   */
  Object translateInputsFromPayload(String payload);
  
  /**
   * Translates to DAG format
   */
  DAGNode translateToDAG(String app, String inputs) throws BindingException;
  
  
  /**
   * Get inputs from the inputs
   */
  Object translateInputs(String inputs) throws BindingException;
}