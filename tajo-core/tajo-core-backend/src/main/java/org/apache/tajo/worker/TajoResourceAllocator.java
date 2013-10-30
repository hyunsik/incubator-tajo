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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeIdPBImpl;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.TajoMasterProtocol;
import org.apache.tajo.master.ContainerProxy;
import org.apache.tajo.master.TajoContainerProxy;
import org.apache.tajo.master.TaskRunnerGroupEvent;
import org.apache.tajo.master.TaskRunnerLauncher;
import org.apache.tajo.master.event.ContainerAllocationEvent;
import org.apache.tajo.master.event.ContainerAllocatorEventType;
import org.apache.tajo.master.event.SubQueryContainerAllocationEvent;
import org.apache.tajo.master.querymaster.QueryMasterTask;
import org.apache.tajo.master.querymaster.SubQuery;
import org.apache.tajo.master.querymaster.SubQueryState;
import org.apache.tajo.master.rm.TajoWorkerContainer;
import org.apache.tajo.master.rm.TajoWorkerContainerId;
import org.apache.tajo.master.rm.WorkerResource;
import org.apache.tajo.rpc.CallFuture;
import org.apache.tajo.rpc.NettyClientBase;
import org.apache.tajo.rpc.RpcConnectionPool;
import org.apache.tajo.util.ApplicationIdUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TajoResourceAllocator extends AbstractResourceAllocator {
  private static final Log LOG = LogFactory.getLog(TajoResourceAllocator.class);

  static AtomicInteger containerIdSeq = new AtomicInteger(0);
  private TajoConf tajoConf;
  private QueryMasterTask.QueryMasterTaskContext queryTaskContext;
  private final ExecutorService executorService;

  private AtomicBoolean stopped = new AtomicBoolean(false);

  public TajoResourceAllocator(QueryMasterTask.QueryMasterTaskContext queryTaskContext) {
    this.queryTaskContext = queryTaskContext;
    executorService = Executors.newFixedThreadPool(
        queryTaskContext.getConf().getIntVar(TajoConf.ConfVars.YARN_RM_TASKRUNNER_LAUNCH_PARALLEL_NUM));
  }

  @Override
  public ContainerId makeContainerId(YarnProtos.ContainerIdProto containerIdProto) {
    TajoWorkerContainerId containerId = new TajoWorkerContainerId();
    ApplicationAttemptId appAttemptId = new ApplicationAttemptIdPBImpl(containerIdProto.getAppAttemptId());
    containerId.setApplicationAttemptId(appAttemptId);
    containerId.setId(containerIdProto.getId());
    return containerId;
  }

  @Override
  public void allocateTaskWorker() {
  }

  @Override
  public int calculateNumRequestContainers(TajoWorker.WorkerContext workerContext, int numTasks) {
    int clusterSlots = workerContext.getNumClusterSlots();
    return clusterSlots == 0 ? 1: Math.min(numTasks, clusterSlots);
  }

  @Override
  public void init(Configuration conf) {
    tajoConf = (TajoConf)conf;

    queryTaskContext.getDispatcher().register(TaskRunnerGroupEvent.EventType.class, new TajoTaskRunnerLauncher());
//
    queryTaskContext.getDispatcher().register(ContainerAllocatorEventType.class, new TajoWorkerAllocationHandler());

    super.init(conf);
  }

  @Override
  public synchronized void stop() {
    if(stopped.get()) {
      return;
    }
    stopped.set(true);
    executorService.shutdownNow();

    Map<ContainerId, ContainerProxy> containers = queryTaskContext.getResourceAllocator().getContainers();
    List<ContainerProxy> list = new ArrayList<ContainerProxy>(containers.values());
    for(ContainerProxy eachProxy: list) {
      try {
        eachProxy.stopContainer();
      } catch (Exception e) {
      }
    }
    super.stop();
  }

  @Override
  public void start() {
    super.start();
  }

  final public static FsPermission QUERYCONF_FILE_PERMISSION =
      FsPermission.createImmutable((short) 0644); // rw-r--r--

  private static void writeConf(Configuration conf, Path queryConfFile)
      throws IOException {
    // Write job file to Tajo's fs
    FileSystem fs = queryConfFile.getFileSystem(conf);
    FSDataOutputStream out =
        FileSystem.create(fs, queryConfFile,
            new FsPermission(QUERYCONF_FILE_PERMISSION));
    try {
      conf.writeXml(out);
    } finally {
      out.close();
    }
  }

  class TajoTaskRunnerLauncher implements TaskRunnerLauncher {
    @Override
    public void handle(TaskRunnerGroupEvent event) {
      if (event.getType() == TaskRunnerGroupEvent.EventType.CONTAINER_REMOTE_LAUNCH) {
        launchTaskRunners(event.getExecutionBlockId(), event.getContainers());
      } else if (event.getType() == TaskRunnerGroupEvent.EventType.CONTAINER_REMOTE_CLEANUP) {
        stopContainers(event.getContainers());
      }
    }
  }

  private void launchTaskRunners(ExecutionBlockId executionBlockId, Collection<Container> containers) {
    // Query in standby mode doesn't need launch Worker.
    // But, Assign ExecutionBlock to assigned tajo worker
    for(Container eachContainer: containers) {
      TajoContainerProxy containerProxy = new TajoContainerProxy(queryTaskContext, tajoConf,
          eachContainer, executionBlockId);
      executorService.submit(new LaunchRunner(eachContainer.getId(), containerProxy));
    }
  }

  protected class LaunchRunner implements Runnable {
    private final ContainerProxy proxy;
    private final ContainerId id;
    public LaunchRunner(ContainerId id, ContainerProxy proxy) {
      this.proxy = proxy;
      this.id = id;
    }
    @Override
    public void run() {
      proxy.launch(null);
      LOG.info("ContainerProxy started:" + id);
    }
  }

  private void stopContainers(Collection<Container> containers) {
    for (Container container : containers) {
      final ContainerProxy proxy = queryTaskContext.getResourceAllocator().getContainer(container.getId());
      executorService.submit(new StopContainerRunner(container.getId(), proxy));
    }
  }

  private class StopContainerRunner implements Runnable {
    private final ContainerProxy proxy;
    private final ContainerId id;
    public StopContainerRunner(ContainerId id, ContainerProxy proxy) {
      this.id = id;
      this.proxy = proxy;
    }

    @Override
    public void run() {
      LOG.info("ContainerProxy stopped:" + id + "," + proxy.getId());
      proxy.stopContainer();
    }
  }

  class TajoWorkerAllocationHandler implements EventHandler<ContainerAllocationEvent> {
    @Override
    public void handle(ContainerAllocationEvent event) {
      executorService.submit(new TajoWorkerAllocationThread(event));
    }
  }

  class TajoWorkerAllocationThread extends Thread {
    ContainerAllocationEvent event;
    TajoWorkerAllocationThread(ContainerAllocationEvent event) {
      this.event = event;
    }

    @Override
    public void run() {
      LOG.info("Start TajoWorkerAllocationThread");
      CallFuture<TajoMasterProtocol.WorkerResourceAllocationResponse> callBack =
          new CallFuture<TajoMasterProtocol.WorkerResourceAllocationResponse>();

      int requiredMemoryMBSlot = 512;  //TODO
      int requiredDiskSlots = 1;  //TODO
      TajoMasterProtocol.WorkerResourceAllocationRequest request =
          TajoMasterProtocol.WorkerResourceAllocationRequest.newBuilder()
              .setMemoryMBSlots(requiredMemoryMBSlot)
              .setDiskSlots(requiredDiskSlots)
              .setNumWorks(event.getRequiredNum())
              .setExecutionBlockId(event.getExecutionBlockId().getProto())
              .build();

      RpcConnectionPool connPool = RpcConnectionPool.getPool(queryTaskContext.getConf());
      NettyClientBase tmClient = null;
      try {
        tmClient = connPool.getConnection(
            queryTaskContext.getQueryMasterContext().getWorkerContext().getTajoMasterAddress(),
            TajoMasterProtocol.class, true);
        TajoMasterProtocol.TajoMasterProtocolService masterClientService = tmClient.getStub();
        masterClientService.allocateWorkerResources(null, request, callBack);
      } catch (Exception e) {
        connPool.closeConnection(tmClient);
        tmClient = null;
        LOG.error(e.getMessage(), e);
      } finally {
        connPool.releaseConnection(tmClient);
      }

      TajoMasterProtocol.WorkerResourceAllocationResponse response = null;
      while(!stopped.get()) {
        try {
          response = callBack.get(3, TimeUnit.SECONDS);
          break;
        } catch (InterruptedException e) {
          if(stopped.get()) {
            return;
          }
        } catch (TimeoutException e) {
          LOG.info("No available worker resource for " + event.getExecutionBlockId());
          continue;
        }
      }
      int numAllocatedWorkers = 0;

      if(response != null) {
        List<TajoMasterProtocol.WorkerAllocatedResource> workerHosts = response.getWorkerAllocatedResourceList();
        ExecutionBlockId executionBlockId = event.getExecutionBlockId();

        List<Container> containers = new ArrayList<Container>();
        for(TajoMasterProtocol.WorkerAllocatedResource eachWorker: workerHosts) {
          TajoWorkerContainer container = new TajoWorkerContainer();
          NodeIdPBImpl nodeId = new NodeIdPBImpl();

          nodeId.setHost(eachWorker.getWorkerHost());
          nodeId.setPort(eachWorker.getPeerRpcPort());

          TajoWorkerContainerId containerId = new TajoWorkerContainerId();

          containerId.setApplicationAttemptId(
              ApplicationIdUtils.createApplicationAttemptId(executionBlockId.getQueryId()));
          containerId.setId(containerIdSeq.incrementAndGet());

          container.setId(containerId);
          container.setNodeId(nodeId);

          WorkerResource workerResource = new WorkerResource();
          workerResource.setAllocatedHost(nodeId.getHost());
          workerResource.setPeerRpcPort(nodeId.getPort());
          workerResource.setQueryMasterPort(eachWorker.getQueryMasterPort());
          workerResource.setPullServerPort(eachWorker.getWorkerPullServerPort());
          workerResource.setMemoryMBSlots(requiredMemoryMBSlot);
          workerResource.setDiskSlots(requiredDiskSlots);

          container.setWorkerResource(workerResource);

          containers.add(container);
        }

        SubQueryState state = queryTaskContext.getSubQuery(executionBlockId).getState();
        if (!SubQuery.isRunningState(state)) {
          List<WorkerResource> workerResources = new ArrayList<WorkerResource>();
          for(Container eachContainer: containers) {
            workerResources.add(((TajoWorkerContainer)eachContainer).getWorkerResource());
          }
          try {
            TajoContainerProxy.releaseWorkerResource(queryTaskContext, executionBlockId, workerResources);
          } catch (Exception e) {
            LOG.error(e.getMessage(), e);
          }
          return;
        }

        if (workerHosts.size() > 0) {
          if(LOG.isDebugEnabled()) {
            LOG.debug("SubQueryContainerAllocationEvent fire:" + executionBlockId);
          }
          queryTaskContext.getEventHandler().handle(new SubQueryContainerAllocationEvent(executionBlockId, containers));
        }
        numAllocatedWorkers += workerHosts.size();

      }
      if(event.getRequiredNum() > numAllocatedWorkers) {
        ContainerAllocationEvent shortRequestEvent = new ContainerAllocationEvent(
            event.getType(), event.getExecutionBlockId(), event.getPriority(),
            event.getResource(),
            event.getRequiredNum() - numAllocatedWorkers,
            event.isLeafQuery(), event.getProgress()
        );
        queryTaskContext.getEventHandler().handle(shortRequestEvent);

      }
      LOG.info("Stop TajoWorkerAllocationThread");
    }
  }
}
