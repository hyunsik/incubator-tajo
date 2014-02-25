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

package org.apache.tajo.master.rm;

import com.google.common.base.Preconditions;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.NodeResourceTracker;
import org.apache.tajo.ipc.TajoMasterProtocol;
import org.apache.tajo.rpc.AsyncRpcServer;
import org.apache.tajo.util.NetUtils;
import org.apache.tajo.util.ProtoBufUtil;

import java.io.IOError;
import java.net.InetSocketAddress;

import static org.apache.tajo.ipc.NodeResourceTracker.NodeHeartbeat;
import static org.apache.tajo.ipc.NodeResourceTracker.NodeResourceTrackerService;
import static org.apache.tajo.ipc.TajoMasterProtocol.TajoHeartbeatResponse;

/**
 * It tracks worker and their status. Its has two main roles as follows:
 * <ul>
 *   <li>Membership management for nodes which join to a Tajo cluster</li>
 *   <ul>
 *    <li>Register - It receives the ping from a new worker. It registers the worker.</li>
 *    <li>Unregister - It unregisters a worker who does not send ping for some expiry time.</li>
 *   <ul>
 *   <li>Status Update - It updates the status of all participating workers</li>
 * </ul>
 */
public class WorkerTrackerService extends AbstractService implements NodeResourceTrackerService.Interface {
  private Log LOG = LogFactory.getLog(WorkerTrackerService.class);

  private final WorkerRMContext rmContext;
  private final WorkerLivelinessMonitor workerLivelinessMonitor;

  private AsyncRpcServer server;
  private InetSocketAddress bindAddress;

  public WorkerTrackerService(WorkerRMContext rmContext, WorkerLivelinessMonitor workerLivelinessMonitor) {
    super(WorkerTrackerService.class.getSimpleName());
    this.rmContext = rmContext;
    this.workerLivelinessMonitor = workerLivelinessMonitor;
  }

  @Override
  public void serviceInit(Configuration conf) {
    Preconditions.checkArgument(conf instanceof TajoConf, "conf must be a TajoConf instance");
    TajoConf systemConf = (TajoConf) conf;

    String confMasterServiceAddr = systemConf.getVar(TajoConf.ConfVars.RESOURCE_TRACKER_RPC_ADDRESS);
    InetSocketAddress initIsa = NetUtils.createSocketAddr(confMasterServiceAddr);

    try {
      server = new AsyncRpcServer(NodeResourceTracker.class, this, initIsa, 3);
    } catch (Exception e) {
      LOG.error(e);
      throw new IOError(e);
    }
    server.start();
    bindAddress = NetUtils.getConnectAddress(server.getListenAddress());
    systemConf.setVar(TajoConf.ConfVars.RESOURCE_TRACKER_RPC_ADDRESS,
        NetUtils.normalizeInetSocketAddress(bindAddress));

    LOG.info("NodeResourceTracker starts up (" + this.bindAddress + ")");
    super.start();
  }

  @Override
  public void serviceStop() {
    if(server != null) {
      server.shutdown();
      server = null;
    }
    super.stop();
  }

  @Override
  public void heartbeat(
      RpcController controller,
      NodeHeartbeat heartbeat,
      RpcCallback<TajoHeartbeatResponse> done) {

    try {
      synchronized (this) {
        String workerKey = createWorkerId(heartbeat);
        LOG.info("Heartbeat from " + workerKey);

        if(rmContext.getWorkers().containsKey(workerKey)) { // worker is running
          LOG.info("Worker is running");
          rmContext.getDispatcher().getEventHandler().handle(
              new WorkerStatusEvent(
                  workerKey,
                  heartbeat.getServerStatus().getRunningTaskNum(),
                  heartbeat.getServerStatus().getJvmHeap().getMaxHeap(),
                  heartbeat.getServerStatus().getJvmHeap().getFreeHeap(),
                  heartbeat.getServerStatus().getJvmHeap().getTotalHeap()));
          workerLivelinessMonitor.receivedPing(workerKey);
        } else if (rmContext.getInactiveWorkers().containsKey(workerKey)) { // worker is dead
          LOG.info("Worker is inactive");
          Worker worker = rmContext.getInactiveWorkers().remove(workerKey);
          workerLivelinessMonitor.unregister(worker.getWorkerId());

          Worker newWorker = createWorkerResource(heartbeat);
          String workerId = newWorker.getWorkerId();
          rmContext.getWorkers().putIfAbsent(workerId, newWorker);
          rmContext.getDispatcher().getEventHandler().handle(new WorkerEvent(workerId, WorkerEventType.STARTED));

          workerLivelinessMonitor.register(workerId);

        } else { // new worker
          LOG.info("Worker is new");
          Worker newWorker = createWorkerResource(heartbeat);

          String workerId = newWorker.getWorkerId();
          Worker oldWorker = rmContext.getWorkers().putIfAbsent(workerId, newWorker);

          if (oldWorker == null) {
            rmContext.rmDispatcher.getEventHandler().handle(new WorkerEvent(workerId, WorkerEventType.STARTED));
          } else {
            LOG.info("Reconnect from the node at: " + workerId);
            workerLivelinessMonitor.unregister(workerId);
            rmContext.getDispatcher().getEventHandler().handle(new WorkerReconnectEvent(workerId, newWorker));
          }

          workerLivelinessMonitor.register(workerKey);
        }

        notifyAll();
      }
    } finally {
      TajoHeartbeatResponse.Builder builder = TajoHeartbeatResponse.newBuilder().setHeartbeatResult(ProtoBufUtil.TRUE);
      builder.setClusterResourceSummary(getClusterResourceSummary());
      done.run(builder.build());
    }
  }

  private static final String createWorkerId(NodeHeartbeat heartbeat) {
    return heartbeat.getTajoWorkerHost() + ":" + heartbeat.getTajoQueryMasterPort() + ":"
        + heartbeat.getPeerRpcPort();
  }

  private Worker createWorkerResource(NodeHeartbeat request) {
    boolean queryMasterMode = request.getServerStatus().getQueryMasterMode().getValue();
    boolean taskRunnerMode = request.getServerStatus().getTaskRunnerMode().getValue();

    WorkerResource workerResource = new WorkerResource();
    workerResource.setQueryMasterMode(queryMasterMode);
    workerResource.setTaskRunnerMode(taskRunnerMode);

    workerResource.setLastHeartbeat(System.currentTimeMillis());
    if(request.getServerStatus() != null) {
      workerResource.setMemoryMB(request.getServerStatus().getMemoryResourceMB());
      workerResource.setCpuCoreSlots(request.getServerStatus().getSystem().getAvailableProcessors());
      workerResource.setDiskSlots(request.getServerStatus().getDiskSlots());
      workerResource.setNumRunningTasks(request.getServerStatus().getRunningTaskNum());
      workerResource.setMaxHeap(request.getServerStatus().getJvmHeap().getMaxHeap());
      workerResource.setFreeHeap(request.getServerStatus().getJvmHeap().getFreeHeap());
      workerResource.setTotalHeap(request.getServerStatus().getJvmHeap().getTotalHeap());
    } else {
      workerResource.setMemoryMB(4096);
      workerResource.setDiskSlots(4);
      workerResource.setCpuCoreSlots(4);
    }

    Worker worker = new Worker(rmContext, workerResource);
    worker.setAllocatedHost(request.getTajoWorkerHost());
    worker.setHttpPort(request.getTajoWorkerHttpPort());
    worker.setPeerRpcPort(request.getPeerRpcPort());
    worker.setQueryMasterPort(request.getTajoQueryMasterPort());
    worker.setClientPort(request.getTajoWorkerClientPort());
    worker.setPullServerPort(request.getTajoWorkerPullServerPort());
    return worker;
  }

  public TajoMasterProtocol.ClusterResourceSummary getClusterResourceSummary() {
    int totalDiskSlots = 0;
    int totalCpuCoreSlots = 0;
    int totalMemoryMB = 0;

    int totalAvailableDiskSlots = 0;
    int totalAvailableCpuCoreSlots = 0;
    int totalAvailableMemoryMB = 0;

    synchronized(rmContext) {
      for(String eachWorker: rmContext.getWorkers().keySet()) {
        Worker worker = rmContext.getWorkers().get(eachWorker);
        WorkerResource resource = worker.getResource();
        if(worker != null) {
          totalMemoryMB += resource.getMemoryMB();
          totalAvailableMemoryMB += resource.getAvailableMemoryMB();

          totalDiskSlots += resource.getDiskSlots();
          totalAvailableDiskSlots += resource.getAvailableDiskSlots();

          totalCpuCoreSlots += resource.getCpuCoreSlots();
          totalAvailableCpuCoreSlots += resource.getAvailableCpuCoreSlots();
        }
      }
    }

    return TajoMasterProtocol.ClusterResourceSummary.newBuilder()
        .setNumWorkers(rmContext.getWorkers().size())
        .setTotalCpuCoreSlots(totalCpuCoreSlots)
        .setTotalDiskSlots(totalDiskSlots)
        .setTotalMemoryMB(totalMemoryMB)
        .setTotalAvailableCpuCoreSlots(totalAvailableCpuCoreSlots)
        .setTotalAvailableDiskSlots(totalAvailableDiskSlots)
        .setTotalAvailableMemoryMB(totalAvailableMemoryMB)
        .build();
  }
}
