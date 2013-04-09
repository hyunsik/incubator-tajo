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
  <%@ page import="org.apache.hadoop.util.StringUtils" %>

  <%@include file="./header.jsp" %>

  <div class="container-tajo">
  <h2>Tajo Catalog</h2>
  <table>
    <tr><th colspan="2">Table Summary</th></tr>
    <tr>
    <td>Total Tables</td><td><%=catalog.getAllTableNames().size()%></td>
    <td>&nbsp;</td><td>&nbsp;</td>
    </tr>
  </table>

  <table>
    <tr><th colspan="4">Table List</th></tr>
    <tr>
      <th>Name</th>
      <th>Path</th>
      <th>Format</th>
      <th>Volume</th>
    </tr>
    <%
    String[] tableArr = catalog.getAllTableNames().toArray(new String[0]);
    for(int i = 0 ; i < tableArr.length ; i ++ ) {
      TableDesc table = catalog.getTableDesc(tableArr[i]);
    %>
    <tr>
      <td><a href = "./tabledesc.jsp?tablename=<%=table.getId()%>" ><%=table.getId()%></a></td>
      <td><%=table.getPath()%></td>
      <td><%=table.getMeta().getStoreType()%></td>
      <td><%=StringUtils.byteDesc(table.getMeta().getStat().getNumBytes())%></td>
    </tr>
    <% } %>
  </table>
</body>
</html>

