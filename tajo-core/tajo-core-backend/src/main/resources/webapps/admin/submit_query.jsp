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
  <%@ page import="tajo.conf.TajoConf" %>
  <%@ page import="tajo.*" %>
  <%@ page import="tajo.catalog.*" %>
  <%@ page import="tajo.master.TajoMaster" %>
  <%@ page import="tajo.master.QueryMaster" %>
  <%@ page import="tajo.master.ClientService" %>
  <%@ page import="tajo.master.SubQuery" %>
  <%@ page import="tajo.master.Query" %>
  <%@ page import="tajo.engine.*" %>
  <%@ page import="tajo.util.TajoIdUtils" %>
  <%@ page import="java.net.InetSocketAddress" %>
  <%@ page import="java.net.InetAddress"  %>
  <%@ page import="java.text.SimpleDateFormat" %>
  <%@ page import="org.apache.hadoop.conf.Configuration" %>
  <%@ page import="tajo.client.ClientProtocol.*" %>

  <%
    String sql = request.getParameter("sql");
     TajoMaster master = (TajoMaster)application.getAttribute("tajo.master");
     try {
     QueryId queryId = master.getContext().getGlobalEngine().executeQuery(sql);
     response.setStatus(202);
     out.print(queryId.toString());
     } catch (Throwable e) {
      response.setStatus(500);
      out.print(e.getMessage());
     }
  %>