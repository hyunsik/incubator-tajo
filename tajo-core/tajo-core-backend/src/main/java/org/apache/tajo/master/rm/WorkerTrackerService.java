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

import java.io.IOError;
import java.net.InetSocketAddress;

import static org.apache.tajo.ipc.NodeResourceTracker.NodeResourceTrackerService;

public class WorkerTrackerService extends AbstractService {
  private Log LOG = LogFactory.getLog(WorkerTrackerService.class);

  private final WorkerRMContext rmContext;

  private HeartbeatService heartbeatService;
  private AsyncRpcServer server;
  private InetSocketAddress bindAddress;

  public WorkerTrackerService(WorkerRMContext rmContext) {
    super(WorkerTrackerService.class.getSimpleName());
    this.rmContext = rmContext;
  }

  @Override
  public void serviceInit(Configuration conf) {
    Preconditions.checkArgument(conf instanceof TajoConf, "conf must be a TajoConf instance");
    TajoConf systemConf = (TajoConf) conf;

    heartbeatService = new HeartbeatService();

    String confMasterServiceAddr = systemConf.getVar(TajoConf.ConfVars.RESOURCE_TRACKER_RPC_ADDRESS);
    InetSocketAddress initIsa = NetUtils.createSocketAddr(confMasterServiceAddr);

    try {
      server = new AsyncRpcServer(NodeResourceTracker.class, heartbeatService, initIsa, 3);
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

  private static class HeartbeatService implements NodeResourceTrackerService.Interface {

    @Override
    public void heartbeat(RpcController controller,
                          TajoMasterProtocol.TajoHeartbeat request,
                          RpcCallback<TajoMasterProtocol.TajoHeartbeatResponse> done) {

    }
  }
}
