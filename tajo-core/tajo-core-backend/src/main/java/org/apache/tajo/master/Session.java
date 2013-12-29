package org.apache.tajo.master;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

/**
 * Provides a way to identify a user across more than connection or query request to TajoMaster and
 * to store information about that user. The session persists while the user uses the session.
 * If the session is inactive for a expire time, the session will be removed from TajoMaster.
 */
public final class Session {
  private final String sessionId;
  private final String userName;
  private final long creationTime;

  private Long lastAccessedTime;
  private String currentDatabase;

  private final TreeMap<String, Object> variables;

  public Session(String sessionId, String userName, Map<String, Object> defaultVariables) {
    this.sessionId = sessionId;
    this.userName = userName;
    this.creationTime = System.currentTimeMillis();
    this.variables = new TreeMap<String, Object>(defaultVariables);
  }

  /**
   * a string containing the unique identifier assigned to this session
   *
   * @return string representing the unique identifier to this session.
   */
  public String getId() {
    return sessionId;
  }

  public String getUserName() {
    return userName;
  }

  /**
   * Returns the creation time of this session, as the number of milliseconds since midnight January 1, 1970 GMT.
   *
   * @return a long representing the creation time of this session, expressed in milliseconds since 1/1/1970 GMT
   */
  public long getCreationTime() {
    return creationTime;
  }

  /**
   * Returns the last time the client sent a request associated with this session,
   * as the number of milliseconds since midnight January 1, 1970 GMT.
   *
   * @return a long representing the last time the client sent a request associated with this session,
   * expressed in milliseconds since 1/1/1970 GMT
   */
  public long getLastAccessedTime() {
    synchronized (lastAccessedTime) {
      return lastAccessedTime;
    }
  }

  public boolean isExpired() {
    int inActiveInterval = getInactiveInterval();
    return inActiveInterval >= (Integer)getValue(SessionVariable.SESSION_EXPIRE_TIME);
  }

  /**
   * Return inactive interval as seconds
   *
   * @return The seconds for which session have been inactive.
   */
  public int getInactiveInterval() {
    synchronized (lastAccessedTime) {
      return (int)(lastAccessedTime - System.currentTimeMillis()) / 1000;
    }
  }

  /**
   * Return the current database name.
   *
   * @return The current database name.
   */
  public String getCurrentDatabase() {
    synchronized (currentDatabase) {
      return currentDatabase;
    }
  }

  public void setCurrentDatabase(String database) {
    synchronized (currentDatabase) {
      this.currentDatabase = database;
    }
  }

  /**
   * update the last accessed time.
   */
  public void updateLastAccessedTime() {
    synchronized (lastAccessedTime) {
      lastAccessedTime = System.currentTimeMillis();
    }
  }

  public Object getValue(String name) {
    synchronized (variables) {
      return variables.get(name);
    }
  }

  public Object getValue(SessionVariable name) {
    synchronized (variables) {
      return variables.get(name.getName());
    }
  }

  public Collection<String> getValueNames() {
    synchronized (variables) {
      return variables.navigableKeySet();
    }
  }

  public void setValue(String name, String value) {
    synchronized (variables) {
      variables.put(name, value);
    }
  }

  public void setValue(SessionVariable name, String value) {
    synchronized (variables) {
      variables.put(name.getName(), value);
    }
  }

  public void removeValue(String name) {
    synchronized (variables) {
      variables.remove(name);
    }
  }
}
