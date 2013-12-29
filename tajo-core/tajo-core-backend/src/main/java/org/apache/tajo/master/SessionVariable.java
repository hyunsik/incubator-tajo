package org.apache.tajo.master;

import org.apache.tajo.conf.TajoConf;

public enum SessionVariable {
  USERNAME("Username"),
  CLIENT_ADDRESS("Client_Address"),
  SESSION_EXPIRE_TIME("Session_ExpireTime", TajoConf.ConfVars.SESSION_EXPIRE_INTERVAL);

  private final String name;
  private TajoConf.ConfVars defaultVariable;

  SessionVariable(String variableName) {
    this.name = variableName;
  }

  SessionVariable(String variableName, TajoConf.ConfVars confVars) {
    this.name = variableName;
    this.defaultVariable = confVars;
  }

  boolean hasDefaultVariable() {
    return defaultVariable != null;
  }

  public String getName() {
    return name;
  }

  TajoConf.ConfVars getDefaultConfVar() {
    return defaultVariable;
  }
}
