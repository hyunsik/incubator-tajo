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

  <%@include file="./header.jsp" %>

    <%
   String tableName = request.getParameter("tablename");
   if(tableName == null) {
	  if(master.getCatalog().getAllTableNames().iterator().hasNext())
	    tableName = catalog.getAllTableNames().iterator().next();
	  else
		tableName = null;
   }
   TableDesc desc = null;
   TableMeta meta = null;
   Collection<String> tableList = null;
   if(tableName != null) {
	 desc = catalog.getTableDesc(tableName);
     meta = desc.getMeta();
     tableList = master.getCatalog().getAllTableNames();
   }

     if(tableName != null) {
    	 out.write("<h2 class = \"line\" >Table Info</h2>");
    	 TableDesc table = catalog.getTableDesc(tableName);
    	 out.write("<ul>");
    	 out.write("<li> Table Name : " + table.getId() + "</li>");
    	 out.write("<li> Table Path : " + table.getPath() + "</li>");
    	 out.write("<li> Store Type : " + table.getMeta().getStoreType() + "</li>");
    	 out.write("<li> Schema<ul>");
    	 Schema schema = table.getMeta().getSchema();
    	 for(int i = 0 ; i < table.getMeta().getSchema().getColumnNum() ; i ++) {
    		 out.write("<li>" + schema.getColumn(i).toString() + "</li>");
    	 }
    	 out.write("</ul>");
    	 out.write("</li>");
    	 out.write("</ul>");
     }
    %>
  <div class="container-tajo">
    <h2 class = "line">Table List</h2>
    <table align = "center" class = "new">
    <tr>
     <th>TableName</th>
     <th>TablePath</th>
     <th>StoreType</th>
    </tr>
    <%
    String[] tableArr = catalog.getAllTableNames().toArray(new String[0]);
    for(int i = 0 ; i < tableArr.length ; i ++ ) {
      TableDesc table = catalog.getTableDesc(tableArr[i]);    
    %>
    <tr>
      <td><a href = "./catalog.jsp?tablename=<%=table.getId()%>" class = "tablelink"><%=table.getId()%></a></td>
      <td><%=table.getPath()%></td>
      <td><%=table.getMeta().getStoreType()%></td>
    </tr>	
    <% 	
    }
    %>
    </table>
    
    <h2 class = "line">Function List</h2>
    <h2 class = "line">Others</h2>
  </div>
</body>
</html>

