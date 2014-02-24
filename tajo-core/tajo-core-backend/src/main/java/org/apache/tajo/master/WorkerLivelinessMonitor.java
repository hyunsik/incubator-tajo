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

package org.apache.tajo.master;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.AbstractLivelinessMonitor;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.master.rm.WorkerEventType;
import org.apache.tajo.master.rm.WorkerEvent;

public class WorkerLivelinessMonitor extends AbstractLivelinessMonitor<String> {

  private EventHandler dispatcher;

  public WorkerLivelinessMonitor(Dispatcher d) {
    super("NMLivelinessMonitor", new SystemClock());
    this.dispatcher = d.getEventHandler();
  }

  public void serviceInit(Configuration conf) throws Exception {
    Preconditions.checkArgument(conf instanceof TajoConf);
    TajoConf systemConf = (TajoConf) conf;
    int expireIntvl = systemConf.getIntVar(TajoConf.ConfVars.WORKER_HEARTBEAT_TIMEOUT);
    setExpireInterval(expireIntvl);
    setMonitorInterval(expireIntvl/3);
    super.serviceInit(conf);
  }

  @Override
  protected void expire(String id) {
    dispatcher.handle(new WorkerEvent(id, WorkerEventType.EXPIRE));
  }
}
