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
  <%@ page import="tajo.catalog.statistics.TableStat" %>
  <%@ page import="tajo.master.TajoMaster" %>
  <%@ page import="tajo.engine.*" %>
  <%@ page import="java.net.InetSocketAddress" %>
  <%@ page import="java.net.InetAddress"  %>
  <%@ page import="org.apache.hadoop.conf.Configuration" %>
  <%@ page import="org.apache.hadoop.util.StringUtils" %>

  <%@include file="./header.jsp" %>

   <%
    String tableName = request.getParameter("tablename");
    TableDesc desc = catalog.getTableDesc(tableName);
    TableMeta meta = desc.getMeta();
    TableStat stat = meta.getStat();
    long volume = meta.getStat().getNumBytes();
   %>

  <div class="container-tajo">
  <h2><%=tableName%></h2>

  <table>
  <tr><th colspan="4">Table Summary</th></tr>
  <tr>
    <td>Name</td><td><%=tableName%></td>
    <td>Path</td><td><%=desc.getPath()%></td>
  </tr>
  <tr>
    <td>Format</td><td><%=meta.getStoreType()%></td>
    <td>Volume</td><td><%=volume + " (" + StringUtils.byteDesc(volume) +")"%></td>
  </tr>
  </table>

  <table>
  <tr><th colspan="2">Table Schema</th></tr>
  <tr>
    <th>Name</th>
    <th>Domain</th>
  </tr>
    <%
    for (Column column : meta.getSchema().getColumns()) {
    %>
      <tr>
        <td><%=column.getColumnName()%></td><td><%=column.getDataType()%></td>
      </tr>
    <%
    }
    %>
  </table>

  </div> <!-- container-tajo -->
</body>
</html>

