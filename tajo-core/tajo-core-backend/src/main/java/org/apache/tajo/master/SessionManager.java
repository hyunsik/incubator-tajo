package org.apache.tajo.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.util.TUtil;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class SessionManager extends AbstractService {
  private static Log LOG = LogFactory.getLog(SessionManager.class);

  private static final ConcurrentSkipListMap<String, Session> sessions = new ConcurrentSkipListMap<String, Session>();
  private static final ScheduledExecutorService scheduledExecutor = new ScheduledThreadPoolExecutor(1);
  private static TreeMap<String, Object> defaultVariables;

  public SessionManager() {
    super(SessionManager.class.getSimpleName());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    TajoConf systemConf = (TajoConf) conf;
    int expiryCheckInterval = systemConf.getIntVar(TajoConf.ConfVars.SESSION_EXPIRE_CHECK_INTERVAL);
    scheduledExecutor.scheduleWithFixedDelay(new SessionExpiredCheckerThread(), expiryCheckInterval,
        expiryCheckInterval, TimeUnit.SECONDS);
    initializeDefaultVariables(systemConf);
  }

  @Override
  protected void serviceStop() throws Exception {
    scheduledExecutor.shutdownNow();
  }

  private void initializeDefaultVariables(TajoConf systemConf) {
    for (SessionVariable var : SessionVariable.values()) {
      if (var.hasDefaultVariable()) {
        if (var.getDefaultConfVar().getVarClass() == Boolean.class) {
          defaultVariables.put(var.getName(), systemConf.getBoolVar(var.getDefaultConfVar()));
        } else if (var.getDefaultConfVar().getVarClass() == Integer.class) {
          defaultVariables.put(var.getName(), systemConf.getIntVar(var.getDefaultConfVar()));
        } else if (var.getDefaultConfVar().getVarClass() == Long.class) {
          defaultVariables.put(var.getName(), systemConf.getLongVar(var.getDefaultConfVar()));
        } else if (var.getDefaultConfVar().getVarClass() == Float.class) {
          defaultVariables.put(var.getName(), systemConf.getFloatVar(var.getDefaultConfVar()));
        } else if (var.getDefaultConfVar().getVarClass() == String.class) {
          defaultVariables.put(var.getName(), systemConf.getVar(var.getDefaultConfVar()));
        }
      }
    }
  }

  public String createSession(String userName) {
    UUID uniqueId = UUID.randomUUID();
    Session session = new Session(uniqueId.toString(), userName, defaultVariables);
    return session.getId();
  }

  public void removeSession(String id) {
    sessions.remove(id);
  }

  public Session getSession(String id) {
    return sessions.get(id);
  }

  private static class SessionExpiredCheckerThread implements Runnable {

    @Override
    public void run() {
      Iterator<Map.Entry<String,Session>> it = sessions.entrySet().iterator();
      Map.Entry<String, Session> entry;
      Set<String> expiredSessionIds = TUtil.newHashSet();
      while (it.hasNext()) {
        entry = it.next();

        if (entry.getValue().isExpired()) {
          expiredSessionIds.add(entry.getKey());
        }
      }

      for (String sessionId : expiredSessionIds) {
        sessions.remove(sessionId);
      }

      LOG.info(expiredSessionIds.size() + " sessions are expired.");
    }
  }
}
