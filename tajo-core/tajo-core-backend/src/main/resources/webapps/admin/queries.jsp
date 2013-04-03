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
  <%@ page import="java.util.Map.Entry" %>
  <%@ page import="tajo.QueryId" %>
  <%@ page import="java.text.SimpleDateFormat" %>
  <%@ page import="java.util.Date" %>
  <%@ page import="tajo.TajoProtos" %>


  <%@include file="./header.jsp" %>

  <div class ="container-tajo">

  <table>
    <tr><th colspan="6">Running Queries</th></tr>
    <tr>
    <th>Query Id</th>
    <th>Progress</th>
    <th>Start Time</th>
    <th>Finish Time</th>
    <th>Response Time</th>
    <th>Final State</th>
    </tr>
    <%
    SimpleDateFormat df = new SimpleDateFormat("HH:mm:ss");

    for (Entry<QueryId, QueryMaster> query : master.getContext().getAllQueries().entrySet()) {
      QueryMaster qm = query.getValue();
      float progress = qm.getContext().getProgress();
      long startTime = qm.getContext().getStartTime();
      long finishTime = qm.getContext().getFinishTime();
      TajoProtos.QueryState finalState = qm.getContext().getQuery().getState();
      String responseTime;
      if (finalState == TajoProtos.QueryState.QUERY_SUCCEEDED) {
        responseTime = ((finishTime - startTime) / 1000) + " sec";
      } else {
        responseTime = "Nil";
      }
    %>
    <tr>
    <td><a href="queryinfo.jsp?qid=<%=query.getKey()%>"><%=query.getKey()%></a></td>
    <td><%=progress%></td>
    <td><%=df.format(startTime)%></td>
    <td><%=df.format(finishTime)%></td>
    <td><%=responseTime%></td>
    <td><%=finalState%></td>
    </tr>
    <% } %>
  </table>
  </div> <!-- container-tajo -->
  </body>
</html>
