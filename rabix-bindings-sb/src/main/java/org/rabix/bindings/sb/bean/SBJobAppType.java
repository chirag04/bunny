package org.rabix.bindings.sb.bean;

import java.io.Serializable;

public enum SBJobAppType implements Serializable {

  WORKFLOW,
  COMMAND_LINE_TOOL,
  EXPRESSION_TOOL,
  PYTHON_TOOL,
  EMBEDDED

}
