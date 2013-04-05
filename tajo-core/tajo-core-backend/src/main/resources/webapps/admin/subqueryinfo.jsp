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
  <%@ page import="tajo.*" %>
  <%@ page import="tajo.catalog.*" %>
  <%@ page import="tajo.master.TajoMaster" %>
  <%@ page import="tajo.master.QueryMaster" %>
  <%@ page import="tajo.master.SubQuery" %>
  <%@ page import="tajo.engine.*" %>
  <%@ page import="tajo.util.TajoIdUtils" %>
  <%@ page import="java.net.InetSocketAddress" %>
  <%@ page import="java.net.InetAddress"  %>
  <%@ page import="org.apache.hadoop.conf.Configuration" %>

  <%@include file="./header.jsp" %>

  <%
    String subQueryIdStr = request.getParameter("subqid");
    SubQueryId subQid = TajoIdUtils.newSubQueryId(subQueryIdStr);
    QueryId qid = subQid.getQueryId();
    QueryMaster qm = master.getContext().getQuery(qid);
    SubQuery subQuery = qm.getContext().getQuery().getSubQuery(subQid);
  %>

  <div class ="container-tajo">
    <table>
      <tr><th colspan="2">SubQuery Info : <%=subQueryIdStr%></th></tr>

      <tr>
      <td>Logical Plan</td><td><pre><%=subQuery.getLogicalPlan().toString()%></pre></td>
      </tr>
      <tr>
      <td>State</td><td><pre><%=subQuery.getState().name()%></pre></td>
      </tr>
    </table>
  </div>