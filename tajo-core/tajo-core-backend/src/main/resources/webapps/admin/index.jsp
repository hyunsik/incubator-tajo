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



  <div class="container-tajo">
     <div class = "outbox">
      <h2 class = "compactline">System Summary</h2>
      <table align = "center"class = "noborder">
       <tr>
        <th class="rightbottom">Cluster Number</th>
        <th class="rightbottom">Live workers</th>
        <th class="rightbottom">Table number</th>
        <th class="rightbottom">Total Disk Size</th>
        <th class="rightbottom">Available Disk Size</th>
        <th class="bottom">Running Time</th>
       </tr>
       <tr>
        <td class="rightborder"><%//=map.size()%></td>
        <td class="rightborder"><%//=curMap.size()%></td>
        <td class="rightborder"><%//=tableArr.length%></td>
        <td class="rightborder"><%//=String.format("%.2f", totalDisk/MB)%>TB</td>
        <td class="rightborder"><%//=String.format("%.2f", availableDisk/MB)%>TB</td>
        <td class="noborder"><%//=time/3600/24 + "d" + time/3600 + "h" + time/60%60 + "m" + time%60+"s"%></td>
       </tr>
      </table>
     </div>
    
     <div class="outbox_order">
      <h2 class="compactline">Table List</h2>     
      <table align = "center" class = "new">
      <tr>
       <th style="width:110px">TableName</th>
       <th>TablePath</th>
      </tr>

      <tr>
        <td><a href = "catalog.jsp?tablename=<%//=table.getId()%>" class = "tablelink"><%//=table.getId()%></a></td>
        <td><%//=table.getPath()%></td>
      </tr>	

      </table>
     </div>
     <div style="float:left; width:6px">&nbsp;</div>
     <div class="outbox_order">
      <h2 class="compactline">Worker List</h2>
      <table align="center" class = "new">
      <tr>
        <th style="width:90px">Status</th>
        <th>Worker Name</th>
      </tr>
     </table> 
    </div>
    <div style="clear:both"></div>
  </div>

  </div>
  </body>
</html>
