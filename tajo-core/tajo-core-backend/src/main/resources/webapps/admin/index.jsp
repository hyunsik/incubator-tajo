<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
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
<%@ page import="org.apache.hadoop.fs.FileSystem" %>
<%@ page import="org.apache.hadoop.hdfs.DistributedFileSystem" %>
<%@ page import="org.apache.hadoop.fs.FsStatus" %>
<%@ page import="org.apache.hadoop.hdfs.protocol.HdfsConstants" %>
<%@ page import="org.apache.hadoop.hdfs.protocol.HdfsConstants.*" %>
<%@ page import="org.apache.hadoop.hdfs.protocol.DatanodeInfo" %>
<%@ page import="org.apache.hadoop.util.StringUtils" %>


<%@include file="./header.jsp" %>

  <%
    DistributedFileSystem dfs = (DistributedFileSystem)FileSystem.get(conf);
    FsStatus ds = dfs.getStatus();
    long capacity = ds.getCapacity();
    long used = ds.getUsed();
    long remaining = ds.getRemaining();
    long presentCapacity = used + remaining;
    boolean safeMode = dfs.isInSafeMode();

    DatanodeInfo[] live = dfs.getDataNodeStats(DatanodeReportType.LIVE);
    DatanodeInfo[] dead = dfs.getDataNodeStats(DatanodeReportType.DEAD);
  %>

  <div class="container-tajo">

  <h2>Tajo Cluster Summary</h2>
  <table>
    <tr><th colspan="4">Tajo Summary</th></tr>
    <tr>
    <td>Tables</td><td><%=catalog.getAllTableNames().size()%></td>
    <td>Number of Submitted Queries</td><td><%=master.getContext().getAllQueries().size()%></td>
    </tr>

    <tr>
    <td>Running Queries</td><td>00</td>
    <td>tajo.rootdir</td><td><%=conf.get("tajo.rootdir")%></td>
    </tr>
  </table>

  <table>
    <tr><th colspan="4">System Summary</th></tr>
    <tr>
      <td>Operating System</td><td>Linux 3.2.1-gentoo-r2 (Gentoo Linux)</td>
      <td>Tajo Version</td><td>0.2.0-SNAPSHOT</td>
    </tr>
    <tr>
      <td>Hadoop HDFS Version</td><td>2.0.3-alpha</td></td>
      <td>Hadoop Yarn Version</td><td>2.0.3-alpha</td></td>
    </tr>
  </table>

  <table>
    <tr><th colspan="4">HDFS Summary</th></tr>
      <tr>
        <td>fs.defaultFS</td>
        <td><%=conf.get("fs.defaultFS")%></td>
      </tr>
      <tr>
      <td>Configured Capacity</td><td><%=StringUtils.byteDesc(capacity)%></td>
      <td>Present Capacity</td><td><%=StringUtils.byteDesc(presentCapacity)%></td>
      </tr>

      <tr>
      <td>DFS Remaining</td><td><%=StringUtils.byteDesc(remaining)%></td>
      <td>DFS Used</td><td><%=StringUtils.byteDesc(used)%></td>
      </tr>

      <tr>
      <td>DFS Used%</td><td><%=StringUtils.formatPercent(used/(double)presentCapacity, 2)%></td>
      <td>&nbsp;</td><td>&nbsp;</td>
      </tr>

      <tr>
      <td>Datanodes available</td><td><%=live.length%></td>
      <td>Total Datanodes</td><td><%=(live.length + dead.length)%></td>
      </tr>
  </table>

  <table>
    <tr><th colspan="4">Resource Summary</th></tr>
    <tr>
    <td>Active Nodes</td><td>32</td>
    <td>Running Nodes</td><td>32</td>
    </tr>

    <tr>
    <td>Memory Used</td><td></td>
    <td>Total Memory</td><td></td>
    </tr>
  </table>

  </div> <!-- container-tajo -->
  </body>
</html>
