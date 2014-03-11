/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.master.session;

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;

public class Session implements SessionConstants {
  private final String sessionId;
  private final String userName;
  private long lastAccessTime;
  private final Map<String, String> sessionVariables = new HashMap<String, String>();

  public Session(String sessionId, String userName, String databaseName) {
    this.sessionId = sessionId;
    this.userName = userName;
    this.lastAccessTime = System.currentTimeMillis();

    selectDatabase(databaseName);
  }

  public String getSessionId() {
    return sessionId;
  }

  public String getUserName() {
    return userName;
  }

  public void updateLastAccessTime() {
    lastAccessTime = System.currentTimeMillis();
  }

  public long getLastAccessTime() {
    return lastAccessTime;
  }

  public void setVariable(String name, String value) {
    synchronized (sessionVariables) {
      sessionVariables.put(name, value);
    }
  }

  public String getVariable(String name) throws NoSuchSessionVariableException {
    synchronized (sessionVariables) {
      if (sessionVariables.containsKey(name)) {
        return sessionVariables.get(name);
      } else {
        throw new NoSuchSessionVariableException(name);
      }
    }
  }

  public synchronized Map<String, String> getAllVariables() {
    synchronized (sessionVariables) {
      return ImmutableMap.copyOf(sessionVariables);
    }
  }

  public void selectDatabase(String databaseName) {
    synchronized (sessionVariables) {
      sessionVariables.put(CURRENT_DATABASE, databaseName);
    }
  }

  public String getCurrentDatabase() {
    synchronized (sessionVariables) {
      return sessionVariables.get(CURRENT_DATABASE);
    }
  }
}
