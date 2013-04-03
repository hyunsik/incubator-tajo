<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<%--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
--%>

  <%@ page import="java.util.*" %>
  <%@ page import="tajo.webapp.StaticHttpServer" %>
  <%@ page import="tajo.catalog.*" %>
  <%@ page import="tajo.master.TajoMaster" %>
  <%@ page import="tajo.engine.*" %>
  <%@ page import="java.net.InetSocketAddress" %>
  <%@ page import="java.net.InetAddress"  %>
  <%@ page import="org.apache.hadoop.conf.Configuration" %>
  <%@ page import="org.apache.hadoop.yarn.client.YarnClient" %>
  <%@ page import="org.apache.hadoop.yarn.conf.YarnConfiguration" %>
  <%@ page import="java.net.InetSocketAddress" %>
  <%@ page import="org.apache.hadoop.yarn.ipc.YarnRPC" %>
  <%@ page import="org.apache.hadoop.yarn.api.ClientRMProtocol" %>
  <%@ page import="org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest" %>
  <%@ page import="org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse" %>
  <%@ page import="org.apache.hadoop.yarn.api.records.NodeReport" %>
  <%@ page import="org.apache.hadoop.yarn.util.Records" %>
  <%@ page import="java.util.List" %>

  <%@include file="./header.jsp" %>

  <%
    InetSocketAddress rmAddress;
    YarnConfiguration yarnConf = (YarnConfiguration)conf;
    rmAddress= yarnConf.getSocketAddr(YarnConfiguration.RM_ADDRESS,
      YarnConfiguration.DEFAULT_RM_ADDRESS, YarnConfiguration.DEFAULT_RM_PORT);
    YarnRPC rpc = YarnRPC.create(yarnConf);

    ClientRMProtocol proxy = (ClientRMProtocol)rpc.
      getProxy(ClientRMProtocol.class, rmAddress, yarnConf);

    AMRMClient amClient = (AMRMClient)rpc.
      getProxy(AMRMClient.class, rmAddress, yarnConf);

    GetClusterNodesRequest req = Records.newRecord(GetClusterNodesRequest.class);
    GetClusterNodesResponse res = proxy.getClusterNodes(req);

    List<NodeReport> nodes = res.getNodeReports();

    long unusedNodeCapacity;
    long availNodeCapacity;
    long totalNodeCapacity;
    int numContainers;

    for (NodeReport node : nodes) {
      usedNodeCapacity += report.getUsedResource().getMemory();
      availNodeCapacity += report.getAvailableResource().getMemory();
      totalNodeCapacity += ni.getTotalCapability().getMemory();
      numContainers += fs.getNodeReport(ni.getNodeID()).getNumContainers();
    }
  %>
    <div class ="container-tajo">

    <table>
      <tr><th colspan="4">Cluster Summary</th></tr>
      <tr>
      <td>Physical Nodes</td><td><%=nodes.size();%></td>
      <td>Containers</td><td><%=numContainers%></td>
      </tr>
      <tr>
      <td>Format</td><td><%=meta.getStoreType()%></td>
      <td>Volume</td><td><%=volume + " (" + StringUtils.byteDesc(volume) +")"%></td>
      </tr>
    </table>

    <h2 class = "line">Available workers: <%=nodes.size()%></h2>
    <h2 class = "line">Available workers: <%=nodes.get(0).getNumContainers()%></h2>
  <h2 class = "line">Available workers: <%=nodes.get(0).getCapability().getMemory()%></h2>

    </div> <!-- container-tajo -->
  </body>
</html>
