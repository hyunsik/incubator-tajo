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

  <%@include file="./header.jsp" %>

  <div class ="container-tajo">

  <table>
    <tr><th colspan="2">Running Queries</th></tr>
    <%
    for (Entry<QueryId, QueryMaster> query : master.getContext().getAllQueries().entrySet()) {
    %>
    <tr>
      <th>Query Id</th><th></th>
    </tr>
    <tr>
    <td><a href="queryinfo.jsp?qid=<%=query.getKey()%>"><%=query.getKey()%></a></td><td>&nbsp;</td>
    </tr>
    <% } %>
  </table>

    <div class = "command" >
      <form method="post" action="./queries.jsp">
	    <textarea name="command"  class = "command">insert query</textarea>
	    <br />
	    <br />
	    <br />
	    <input type="submit" value="submit" />
      </form>
    </div>
  </div> <!-- container-tajo -->
  </body>
</html>
