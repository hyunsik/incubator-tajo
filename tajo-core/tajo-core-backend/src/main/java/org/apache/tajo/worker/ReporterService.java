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

package org.apache.tajo.worker;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tajo.rpc.NullCallback;
import org.apache.tajo.worker.event.PeriodicReportEvent;
import org.apache.tajo.worker.event.PeriodicReportEventType;
import org.apache.tajo.worker.event.TaskStatusReportEvent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import static org.apache.tajo.ipc.QueryMasterProtocol.QueryMasterProtocolService.Interface;

public class ReporterService extends AbstractService implements EventHandler<PeriodicReportEvent> {
  private Log LOG = LogFactory.getLog(ReporterService.class);
  private Interface masterStub;
  private Thread reportThread;
  private volatile boolean stopped = false;
  private BlockingQueue<PeriodicReportEvent> reportQueue;

  public ReporterService(Interface queryMasterStub) {
    super(ReporterService.class.getSimpleName());
    this.masterStub = queryMasterStub;
    reportQueue = new LinkedBlockingDeque<PeriodicReportEvent>();
  }

  @Override
  public void serviceInit(Configuration conf) {
    reportThread = new Thread(new ReportThread(), "Communication thread");
    reportThread.setDaemon(true);
    reportThread.start();
  }

  @Override
  public void serviceStop() throws InterruptedException {
    if (reportThread != null) {

      stopped = true;

      reportQueue.notify();
      reportThread.interrupt();
      reportThread.join();
    }
  }

  @Override
  public void handle(PeriodicReportEvent event) {
    reportQueue.add(event);
  }

  private class ReportThread implements Runnable {
    @Override
    public void run() {
      final int MAX_RETRIES = 3;
      int remainingRetries = MAX_RETRIES;

      while (!stopped) {
        PeriodicReportEvent event;

        try {
          event = reportQueue.poll(1000, TimeUnit.SECONDS);
          if (event.getType() == PeriodicReportEventType.STATUS) {
            masterStub.statusUpdate(null, ((TaskStatusReportEvent)event).getTaskStatus(), NullCallback.get());
          } else {
            masterStub.ping(null, event.getTaskAttemptId(), NullCallback.get());
          }
        } catch (Throwable t) {
          LOG.info("Communication exception: " + StringUtils.stringifyException(t));
          remainingRetries -=1;
          if (remainingRetries == 0) {
            ReflectionUtils.logThreadInfo(LOG, "Communication exception", 0);
            LOG.warn("Last retry, killing ");
            System.exit(65);
          }
        }
      }
    }
  }
}
