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
  <%@ page import="tajo.master.QueryMaster" %>
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
  <%@ page import="org.apache.hadoop.util.StringUtils" %>
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

    GetClusterNodesRequest req = Records.newRecord(GetClusterNodesRequest.class);
    GetClusterNodesResponse res = proxy.getClusterNodes(req);

    List<NodeReport> nodes = res.getNodeReports();

    int numPhysicalNodes = nodes.size();
    int numContainers = 0;

    long usedMemory = 0, usedVCores = 0;
    long memoryCapacity = 0, vCoresCapacity = 0;


    for (NodeReport node : nodes) {
      numContainers += node.getNumContainers();

      if (node.getUsed() != null) {
        usedMemory += node.getUsed().getMemory();
        usedVCores += node.getUsed().getVirtualCores();
      }
      memoryCapacity += node.getCapability().getMemory();
      vCoresCapacity += node.getCapability().getVirtualCores();

    }

  %>

  <div class ="container-tajo">
  <h2>Cluster Detail</h2>
  <table>
    <tr><th colspan="4">Cluster Summary</th></tr>
    <tr>
      <td>Physical Nodes</td><td><%=nodes.size()%></td>
      <td>Running Containers</td><td><%=numContainers%></td>
    </tr>

    <tr>
      <td>Used Memory</td><td><%=usedMemory + " MB"%></td>
      <td>Total Memory Capacity</td><td><%=memoryCapacity + " MB"%></td>
    </tr>

    <tr>
      <td>Used Virtual Cores</td><td><%=usedVCores%></td>
      <td>Total Virtual Cores</td><td><%=vCoresCapacity%></td>
    </tr>
  </table>

  <table>
    <tr><th colspan="5">Node Detail</th></tr>
    <tr>
      <th>Hostname</th>
      <th>Running Container Num</th>
      <th>Used Memory</th>
      <th>Used Cores</th>
      <th>Healthy</th>
    </tr>
    <% for (NodeReport node : nodes) {%>
      <tr>
        <td><%=node.getNodeId()%></td>
        <td><%=node.getNumContainers()%></td>
        <td><%=node.getUsed() == null ? 0 : node.getUsed().getMemory()%></td>
        <td><%=node.getUsed() == null ? 0 : node.getUsed().getVirtualCores()%></td>
        <td><%=node.getNodeHealthStatus().getIsNodeHealthy()%></td>
      </tr>
  <% }%>
  </table>


    </div> <!-- container-tajo -->
  </body>
</html>
