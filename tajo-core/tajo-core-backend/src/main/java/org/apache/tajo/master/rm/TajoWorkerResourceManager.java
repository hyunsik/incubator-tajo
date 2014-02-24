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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.protobuf.RpcCallback;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryId;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.TajoMasterProtocol;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.master.WorkerLivelinessMonitor;
import org.apache.tajo.master.querymaster.QueryInProgress;
import org.apache.tajo.master.querymaster.QueryJobEvent;
import org.apache.tajo.util.ApplicationIdUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * It manages all resources of tajo workers.
 */
public class TajoWorkerResourceManager extends CompositeService implements WorkerResourceManager {
  private static final Log LOG = LogFactory.getLog(TajoWorkerResourceManager.class);

  static AtomicInteger containerIdSeq = new AtomicInteger(0);

  private TajoMaster.MasterContext masterContext;

  private WorkerRMContext rmContext;

  private Map<QueryId, WorkerResource> queryMasterMap = new HashMap<QueryId, WorkerResource>();

  private final Object workerResourceLock = new Object();

  private String queryIdSeed;

  private WorkerResourceAllocationThread workerResourceAllocator;

  private WorkerLivelinessMonitor workerLivelinessMonitor;

  private BlockingQueue<WorkerResourceRequest> requestQueue;

  private List<WorkerResourceRequest> reAllocationList;

  private AtomicBoolean stopped = new AtomicBoolean(false);

  private float queryMasterDefaultDiskSlot;

  private int queryMasterDefaultMemoryMB;

  private TajoConf systemConf;

  private Map<YarnProtos.ContainerIdProto, AllocatedWorkerResource> allocatedResourceMap = new HashMap<YarnProtos.ContainerIdProto, AllocatedWorkerResource>();

  public TajoWorkerResourceManager(TajoMaster.MasterContext masterContext) {
    super(TajoWorkerResourceManager.class.getSimpleName());
    this.masterContext = masterContext;
  }

  public TajoWorkerResourceManager(TajoConf systemConf) {
    super(TajoWorkerResourceManager.class.getSimpleName());
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    Preconditions.checkArgument(conf instanceof TajoConf);
    this.systemConf = (TajoConf) conf;

    AsyncDispatcher dispatcher = new AsyncDispatcher();
    addIfService(dispatcher);

    rmContext = new WorkerRMContext(dispatcher);

    this.queryIdSeed = String.valueOf(System.currentTimeMillis());

    this.queryMasterDefaultDiskSlot = systemConf.getFloatVar(TajoConf.ConfVars.TAJO_QUERYMASTER_DISK_SLOT);

    this.queryMasterDefaultMemoryMB = systemConf.getIntVar(TajoConf.ConfVars.TAJO_QUERYMASTER_MEMORY_MB);

    requestQueue = new LinkedBlockingDeque<WorkerResourceRequest>();
    reAllocationList = new ArrayList<WorkerResourceRequest>();

    workerResourceAllocator = new WorkerResourceAllocationThread();
    workerResourceAllocator.start();

    this.workerLivelinessMonitor = new WorkerLivelinessMonitor(this.rmContext.getDispatcher());
    addIfService(this.workerLivelinessMonitor);

    // Register event handler for Workers
    rmContext.getDispatcher().register(WorkerEventType.class, new WorkerEventDispatcher(rmContext));

    super.serviceInit(systemConf);
  }

  @InterfaceAudience.Private
  public static final class WorkerEventDispatcher implements EventHandler<WorkerEvent> {

    private final WorkerRMContext rmContext;

    public WorkerEventDispatcher(WorkerRMContext rmContext) {
      this.rmContext = rmContext;
    }

    @Override
    public void handle(WorkerEvent event) {
      String workerId = event.getWorkerId();
      Worker node = this.rmContext.getWorkers().get(workerId);
      if (node != null) {
        try {
          node.handle(event);
        } catch (Throwable t) {
          LOG.error("Error in handling event type " + event.getType() + " for node " + workerId, t);
        }
      }
    }
  }

  public Map<String, WorkerResource> getWorkers() {
    Map<String, WorkerResource> map = Maps.newHashMap();
    for (Map.Entry<String, Worker> entry : rmContext.getWorkers().entrySet()) {
      map.put(entry.getKey(), entry.getValue().getResource());
    }
    return map;
  }

  @Override
  public Map<String, Worker> getWorkers2() {
    return ImmutableMap.copyOf(rmContext.getWorkers());
  }

  @Override
  public Map<String, Worker> getInactiveWorkers() {
    return ImmutableMap.copyOf(rmContext.getInactiveWorkers());
  }

  public Collection<String> getQueryMasters() {
    return Collections.unmodifiableSet(rmContext.getQueryMasterWorker());
  }

  @Override
  public TajoMasterProtocol.ClusterResourceSummary getClusterResourceSummary() {
    int totalDiskSlots = 0;
    int totalCpuCoreSlots = 0;
    int totalMemoryMB = 0;

    int totalAvailableDiskSlots = 0;
    int totalAvailableCpuCoreSlots = 0;
    int totalAvailableMemoryMB = 0;

    synchronized(workerResourceLock) {
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

  @Override
  public void serviceStop() throws Exception {
    if(stopped.get()) {
      return;
    }
    stopped.set(true);
    if(workerResourceAllocator != null) {
      workerResourceAllocator.interrupt();
    }

    super.serviceStop();
  }

  @Override
  public WorkerResource allocateQueryMaster(QueryInProgress queryInProgress) {
    return allocateQueryMaster(queryInProgress.getQueryId());
  }

  public WorkerResource allocateQueryMaster(QueryId queryId) {
    synchronized(workerResourceLock) {
      if(rmContext.getQueryMasterWorker().size() == 0) {
        LOG.warn("No available resource for querymaster:" + queryId);
        return null;
      }
      WorkerResource queryMasterWorker = null;
      int minTasks = Integer.MAX_VALUE;
      for(String eachQueryMaster: rmContext.getQueryMasterWorker()) {
        Worker worker = rmContext.getWorkers().get(eachQueryMaster);
        WorkerResource resourceOfQueryMaster = worker.getResource();
        if(resourceOfQueryMaster != null && resourceOfQueryMaster.getNumQueryMasterTasks() < minTasks) {
          queryMasterWorker = resourceOfQueryMaster;
          minTasks = resourceOfQueryMaster.getNumQueryMasterTasks();
        }
      }
      if(queryMasterWorker == null) {
        return null;
      }
      queryMasterWorker.addNumQueryMasterTask(queryMasterDefaultDiskSlot, queryMasterDefaultMemoryMB);
      queryMasterMap.put(queryId, queryMasterWorker);
      LOG.info(queryId + "'s QueryMaster is " + queryMasterWorker);
      return queryMasterWorker;
    }
  }

  @Override
  public void startQueryMaster(QueryInProgress queryInProgress) {
    WorkerResource queryMasterWorkerResource = null;
    synchronized(workerResourceLock) {
      queryMasterWorkerResource = queryMasterMap.get(queryInProgress.getQueryId());
    }

    if(queryMasterWorkerResource != null) {
      AllocatedWorkerResource allocatedWorkerResource = new AllocatedWorkerResource();
      allocatedWorkerResource.workerResource = queryMasterWorkerResource;
      allocatedWorkerResource.allocatedMemoryMB = queryMasterDefaultMemoryMB;
      allocatedWorkerResource.allocatedDiskSlots = queryMasterDefaultDiskSlot;

      startQueryMaster(queryInProgress.getQueryId(), allocatedWorkerResource);
    } else {
      //add queue
      TajoMasterProtocol.WorkerResourceAllocationRequest request =
          TajoMasterProtocol.WorkerResourceAllocationRequest.newBuilder()
            .setExecutionBlockId(QueryIdFactory.newExecutionBlockId(QueryIdFactory.NULL_QUERY_ID, 0).getProto())
            .setNumContainers(1)
            .setMinMemoryMBPerContainer(queryMasterDefaultMemoryMB)
            .setMaxMemoryMBPerContainer(queryMasterDefaultMemoryMB)
            .setMinDiskSlotPerContainer(queryMasterDefaultDiskSlot)
            .setMaxDiskSlotPerContainer(queryMasterDefaultDiskSlot)
            .setResourceRequestPriority(TajoMasterProtocol.ResourceRequestPriority.MEMORY)
            .build();
      try {
        requestQueue.put(new WorkerResourceRequest(queryInProgress.getQueryId(), true, request, null));
      } catch (InterruptedException e) {
      }
    }
  }

  private void startQueryMaster(QueryId queryId, AllocatedWorkerResource workResource) {
    QueryInProgress queryInProgress = masterContext.getQueryJobManager().getQueryInProgress(queryId);
    if(queryInProgress == null) {
      LOG.warn("No QueryInProgress while starting  QueryMaster:" + queryId);
      return;
    }
    queryInProgress.getQueryInfo().setQueryMasterResource(workResource.workerResource);

    //fire QueryJobStart event
    queryInProgress.getEventHandler().handle(
        new QueryJobEvent(QueryJobEvent.Type.QUERY_JOB_START, queryInProgress.getQueryInfo()));
  }

  /**
   *
   * @return The prefix of queryId. It is generated when a TajoMaster starts up.
   */
  @Override
  public String getSeedQueryId() throws IOException {
    return queryIdSeed;
  }

  @Override
  public void allocateWorkerResources(
      TajoMasterProtocol.WorkerResourceAllocationRequest request,
      RpcCallback<TajoMasterProtocol.WorkerResourceAllocationResponse> callBack) {
    try {
      //TODO checking queue size
      requestQueue.put(new WorkerResourceRequest(
          new QueryId(request.getExecutionBlockId().getQueryId()), false, request, callBack));
    } catch (InterruptedException e) {
      LOG.error(e.getMessage(), e);
    }
  }

  static class WorkerResourceRequest {
    boolean queryMasterRequest;
    QueryId queryId;
    TajoMasterProtocol.WorkerResourceAllocationRequest request;
    RpcCallback<TajoMasterProtocol.WorkerResourceAllocationResponse> callBack;
    WorkerResourceRequest(
        QueryId queryId,
        boolean queryMasterRequest, TajoMasterProtocol.WorkerResourceAllocationRequest request,
        RpcCallback<TajoMasterProtocol.WorkerResourceAllocationResponse> callBack) {
      this.queryId = queryId;
      this.queryMasterRequest = queryMasterRequest;
      this.request = request;
      this.callBack = callBack;
    }
  }

  static class AllocatedWorkerResource {
    WorkerResource workerResource;
    int allocatedMemoryMB;
    float allocatedDiskSlots;
  }

  class WorkerResourceAllocationThread extends Thread {
    @Override
    public void run() {
      LOG.info("WorkerResourceAllocationThread start");
      while(!stopped.get()) {
        try {
          WorkerResourceRequest resourceRequest = requestQueue.take();

          if (LOG.isDebugEnabled()) {
            LOG.debug("allocateWorkerResources:" +
                (new ExecutionBlockId(resourceRequest.request.getExecutionBlockId())) +
                ", requiredMemory:" + resourceRequest.request.getMinMemoryMBPerContainer() +
                "~" + resourceRequest.request.getMaxMemoryMBPerContainer() +
                ", requiredContainers:" + resourceRequest.request.getNumContainers() +
                ", requiredDiskSlots:" + resourceRequest.request.getMinDiskSlotPerContainer() +
                "~" + resourceRequest.request.getMaxDiskSlotPerContainer() +
                ", queryMasterRequest=" + resourceRequest.queryMasterRequest +
                ", liveWorkers=" + rmContext.getWorkers().size());
          }

          List<AllocatedWorkerResource> allocatedWorkerResources = chooseWorkers(resourceRequest);

          if(allocatedWorkerResources.size() > 0) {
            if(resourceRequest.queryMasterRequest) {
              startQueryMaster(resourceRequest.queryId, allocatedWorkerResources.get(0));
            } else {
              List<TajoMasterProtocol.WorkerAllocatedResource> allocatedResources =
                  new ArrayList<TajoMasterProtocol.WorkerAllocatedResource>();

              for(AllocatedWorkerResource eachWorker: allocatedWorkerResources) {
                NodeId nodeId = NodeId.newInstance(eachWorker.workerResource.getAllocatedHost(),
                    eachWorker.workerResource.getPeerRpcPort());

                TajoWorkerContainerId containerId = new TajoWorkerContainerId();

                containerId.setApplicationAttemptId(
                    ApplicationIdUtils.createApplicationAttemptId(resourceRequest.queryId));
                containerId.setId(containerIdSeq.incrementAndGet());

                YarnProtos.ContainerIdProto containerIdProto = containerId.getProto();
                allocatedResources.add(TajoMasterProtocol.WorkerAllocatedResource.newBuilder()
                    .setContainerId(containerIdProto)
                    .setNodeId(nodeId.toString())
                    .setWorkerHost(eachWorker.workerResource.getAllocatedHost())
                    .setQueryMasterPort(eachWorker.workerResource.getQueryMasterPort())
                    .setPeerRpcPort(eachWorker.workerResource.getPeerRpcPort())
                    .setWorkerPullServerPort(eachWorker.workerResource.getPullServerPort())
                    .setAllocatedMemoryMB(eachWorker.allocatedMemoryMB)
                    .setAllocatedDiskSlots(eachWorker.allocatedDiskSlots)
                    .build());

                synchronized(workerResourceLock) {
                  allocatedResourceMap.put(containerIdProto, eachWorker);
                }
              }

              resourceRequest.callBack.run(TajoMasterProtocol.WorkerResourceAllocationResponse.newBuilder()
                  .setExecutionBlockId(resourceRequest.request.getExecutionBlockId())
                  .addAllWorkerAllocatedResource(allocatedResources)
                  .build()
              );
            }
          } else {
            if(LOG.isDebugEnabled()) {
              LOG.debug("=========================================");
              LOG.debug("Available Workers");
              for(String liveWorker: rmContext.getWorkers().keySet()) {
                LOG.debug(rmContext.getWorkers().get(liveWorker).toString());
              }
              LOG.debug("=========================================");
            }
            requestQueue.put(resourceRequest);
            Thread.sleep(100);
          }
        } catch(InterruptedException ie) {
          LOG.error(ie);
        }
      }
    }
  }

  private List<AllocatedWorkerResource> chooseWorkers(WorkerResourceRequest resourceRequest) {
    List<AllocatedWorkerResource> selectedWorkers = new ArrayList<AllocatedWorkerResource>();

    int allocatedResources = 0;

    if(resourceRequest.queryMasterRequest) {
      WorkerResource worker = allocateQueryMaster(resourceRequest.queryId);
      if(worker != null) {
        AllocatedWorkerResource allocatedWorkerResource = new AllocatedWorkerResource();
        allocatedWorkerResource.workerResource = worker;
        allocatedWorkerResource.allocatedDiskSlots = queryMasterDefaultDiskSlot;
        allocatedWorkerResource.allocatedMemoryMB = queryMasterDefaultMemoryMB;
        selectedWorkers.add(allocatedWorkerResource);

        return selectedWorkers;
      }
    }

    TajoMasterProtocol.ResourceRequestPriority resourceRequestPriority
        = resourceRequest.request.getResourceRequestPriority();

    if(resourceRequestPriority == TajoMasterProtocol.ResourceRequestPriority.MEMORY) {
      synchronized(workerResourceLock) {
        List<String> randomWorkers = new ArrayList<String>(rmContext.getWorkers().keySet());
        Collections.shuffle(randomWorkers);

        int numContainers = resourceRequest.request.getNumContainers();
        int minMemoryMB = resourceRequest.request.getMinMemoryMBPerContainer();
        int maxMemoryMB = resourceRequest.request.getMaxMemoryMBPerContainer();
        float diskSlot = Math.max(resourceRequest.request.getMaxDiskSlotPerContainer(),
            resourceRequest.request.getMinDiskSlotPerContainer());

        int liveWorkerSize = randomWorkers.size();
        Set<String> insufficientWorkers = new HashSet<String>();
        boolean stop = false;
        boolean checkMax = true;
        while(!stop) {
          if(allocatedResources >= numContainers) {
            break;
          }

          if(insufficientWorkers.size() >= liveWorkerSize) {
            if(!checkMax) {
              break;
            }
            insufficientWorkers.clear();
            checkMax = false;
          }
          int compareAvailableMemory = checkMax ? maxMemoryMB : minMemoryMB;

          for(String eachWorker: randomWorkers) {
            if(allocatedResources >= numContainers) {
              stop = true;
              break;
            }

            if(insufficientWorkers.size() >= liveWorkerSize) {
              break;
            }

            Worker worker = rmContext.getWorkers().get(eachWorker);
            WorkerResource workerResource = worker.getResource();
            if(workerResource.getAvailableMemoryMB() >= compareAvailableMemory) {
              int workerMemory;
              if(workerResource.getAvailableMemoryMB() >= maxMemoryMB) {
                workerMemory = maxMemoryMB;
              } else {
                workerMemory = workerResource.getAvailableMemoryMB();
              }
              AllocatedWorkerResource allocatedWorkerResource = new AllocatedWorkerResource();
              allocatedWorkerResource.workerResource = workerResource;
              allocatedWorkerResource.allocatedMemoryMB = workerMemory;
              if(workerResource.getAvailableDiskSlots() >= diskSlot) {
                allocatedWorkerResource.allocatedDiskSlots = diskSlot;
              } else {
                allocatedWorkerResource.allocatedDiskSlots = workerResource.getAvailableDiskSlots();
              }

              workerResource.allocateResource(allocatedWorkerResource.allocatedDiskSlots,
                  allocatedWorkerResource.allocatedMemoryMB);

              selectedWorkers.add(allocatedWorkerResource);

              allocatedResources++;
            } else {
              insufficientWorkers.add(eachWorker);
            }
          }
        }
      }
    } else {
      synchronized(workerResourceLock) {
        List<String> randomWorkers = new ArrayList<String>(rmContext.getWorkers().keySet());
        Collections.shuffle(randomWorkers);

        int numContainers = resourceRequest.request.getNumContainers();
        float minDiskSlots = resourceRequest.request.getMinDiskSlotPerContainer();
        float maxDiskSlots = resourceRequest.request.getMaxDiskSlotPerContainer();
        int memoryMB = Math.max(resourceRequest.request.getMaxMemoryMBPerContainer(),
            resourceRequest.request.getMinMemoryMBPerContainer());

        int liveWorkerSize = randomWorkers.size();
        Set<String> insufficientWorkers = new HashSet<String>();
        boolean stop = false;
        boolean checkMax = true;
        while(!stop) {
          if(allocatedResources >= numContainers) {
            break;
          }

          if(insufficientWorkers.size() >= liveWorkerSize) {
            if(!checkMax) {
              break;
            }
            insufficientWorkers.clear();
            checkMax = false;
          }
          float compareAvailableDisk = checkMax ? maxDiskSlots : minDiskSlots;

          for(String eachWorker: randomWorkers) {
            if(allocatedResources >= numContainers) {
              stop = true;
              break;
            }

            if(insufficientWorkers.size() >= liveWorkerSize) {
              break;
            }

            Worker worker = rmContext.getWorkers().get(eachWorker);
            WorkerResource workerResource = worker.getResource();
            if(workerResource.getAvailableDiskSlots() >= compareAvailableDisk) {
              float workerDiskSlots;
              if(workerResource.getAvailableDiskSlots() >= maxDiskSlots) {
                workerDiskSlots = maxDiskSlots;
              } else {
                workerDiskSlots = workerResource.getAvailableDiskSlots();
              }
              AllocatedWorkerResource allocatedWorkerResource = new AllocatedWorkerResource();
              allocatedWorkerResource.workerResource = workerResource;
              allocatedWorkerResource.allocatedDiskSlots = workerDiskSlots;

              if(workerResource.getAvailableMemoryMB() >= memoryMB) {
                allocatedWorkerResource.allocatedMemoryMB = memoryMB;
              } else {
                allocatedWorkerResource.allocatedMemoryMB = workerResource.getAvailableMemoryMB();
              }
              workerResource.allocateResource(allocatedWorkerResource.allocatedDiskSlots,
                  allocatedWorkerResource.allocatedMemoryMB);

              selectedWorkers.add(allocatedWorkerResource);

              allocatedResources++;
            } else {
              insufficientWorkers.add(eachWorker);
            }
          }
        }
      }
    }
    return selectedWorkers;
  }

  @Override
  public void releaseWorkerResource(ExecutionBlockId ebId, YarnProtos.ContainerIdProto containerId) {
    synchronized(workerResourceLock) {
      AllocatedWorkerResource allocatedWorkerResource = allocatedResourceMap.get(containerId);
      if(allocatedWorkerResource != null) {
        LOG.info("Release Resource:" + ebId + "," +
            allocatedWorkerResource.allocatedDiskSlots + "," + allocatedWorkerResource.allocatedMemoryMB);
        allocatedWorkerResource.workerResource.releaseResource(
            allocatedWorkerResource.allocatedDiskSlots, allocatedWorkerResource.allocatedMemoryMB);
      } else {
        LOG.warn("No AllocatedWorkerResource data for [" + ebId + "," + containerId + "]");
        return;
      }
    }

    synchronized(reAllocationList) {
      reAllocationList.notifyAll();
    }
  }

  @Override
  public boolean isQueryMasterStopped(QueryId queryId) {
    synchronized(workerResourceLock) {
      return !queryMasterMap.containsKey(queryId);
    }
  }

  @Override
  public void stopQueryMaster(QueryId queryId) {
    WorkerResource queryMasterWorkerResource = null;
    synchronized(workerResourceLock) {
      if(!queryMasterMap.containsKey(queryId)) {
        LOG.warn("No QueryMaster resource info for " + queryId);
        return;
      } else {
        queryMasterWorkerResource = queryMasterMap.remove(queryId);
        queryMasterWorkerResource.releaseQueryMasterTask(queryMasterDefaultDiskSlot, queryMasterDefaultMemoryMB);
      }
    }

    LOG.info("release QueryMaster resource:" + queryId + "," + queryMasterWorkerResource);
  }

  public void workerHeartbeat(TajoMasterProtocol.TajoHeartbeat request) {
    synchronized(workerResourceLock) {
      String workerKey = request.getTajoWorkerHost() + ":" + request.getTajoQueryMasterPort() + ":"
          + request.getPeerRpcPort();

      LOG.info("Heartbeat from " + workerKey);
      if(rmContext.getWorkers().containsKey(workerKey)) { // worker is running
        LOG.info("Worker is running");
        Worker worker = rmContext.getWorkers().get(workerKey);
        WorkerResource workerResource = worker.getResource();
        updateWorkerResource(workerResource, request);
        workerLivelinessMonitor.receivedPing(workerKey);

      } else if (rmContext.getInactiveWorkers().containsKey(workerKey)) { // worker is dead
        LOG.info("Worker is inactive");
        Worker worker = rmContext.getInactiveWorkers().remove(workerKey);
        workerLivelinessMonitor.unregister(worker.getWorkerId());

        Worker newWorker = createWorkerResource(request);
        String workerId = newWorker.getWorkerId();
        rmContext.getWorkers().putIfAbsent(workerId, newWorker);
        this.rmContext.getDispatcher().getEventHandler().handle(new WorkerEvent(workerId, WorkerEventType.STARTED));

        workerLivelinessMonitor.register(workerId);
      } else { // new worker
        LOG.info("Worker is new");
        Worker newWorker = createWorkerResource(request);

        String workerId = newWorker.getWorkerId();
        Worker oldWorker = rmContext.getWorkers().putIfAbsent(workerId, newWorker);

        if (oldWorker == null) {
          rmContext.rmDispatcher.getEventHandler().handle(new WorkerEvent(workerId, WorkerEventType.STARTED));
        } else {
          LOG.info("Reconnect from the node at: " + workerId);
          workerLivelinessMonitor.unregister(workerId);
          this.rmContext.getDispatcher().getEventHandler().handle(new WorkerReconnectEvent(workerId, newWorker));
        }

        workerLivelinessMonitor.register(workerKey);
      }

      workerResourceLock.notifyAll();
    }
  }

  private void updateWorkerResource(WorkerResource resource, TajoMasterProtocol.TajoHeartbeat request) {
    resource.setLastHeartbeat(System.currentTimeMillis());
    resource.setWorkerStatus(WorkerStatus.LIVE);
    resource.setNumRunningTasks(request.getServerStatus().getRunningTaskNum());
    resource.setMaxHeap(request.getServerStatus().getJvmHeap().getMaxHeap());
    resource.setFreeHeap(request.getServerStatus().getJvmHeap().getFreeHeap());
    resource.setTotalHeap(request.getServerStatus().getJvmHeap().getTotalHeap());
  }

  private Worker createWorkerResource(TajoMasterProtocol.TajoHeartbeat request) {
    boolean queryMasterMode = request.getServerStatus().getQueryMasterMode().getValue();
    boolean taskRunnerMode = request.getServerStatus().getTaskRunnerMode().getValue();

    WorkerResource workerResource = new WorkerResource();
    workerResource.setAllocatedHost(request.getTajoWorkerHost());
    workerResource.setQueryMasterMode(queryMasterMode);
    workerResource.setTaskRunnerMode(taskRunnerMode);

    workerResource.setQueryMasterPort(request.getTajoQueryMasterPort());
    workerResource.setPeerRpcPort(request.getPeerRpcPort());
    workerResource.setClientPort(request.getTajoWorkerClientPort());
    workerResource.setPullServerPort(request.getTajoWorkerPullServerPort());

    workerResource.setLastHeartbeat(System.currentTimeMillis());
    workerResource.setWorkerStatus(WorkerStatus.LIVE);
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

    Worker worker = new Worker(workerResource.getId(), rmContext, workerResource);
    worker.setHttpPort(request.getTajoWorkerHttpPort());
    return worker;
  }
}
