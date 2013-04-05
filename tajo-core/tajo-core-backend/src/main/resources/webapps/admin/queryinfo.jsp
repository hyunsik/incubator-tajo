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
  <%@ page import="tajo.master.Query" %>
  <%@ page import="tajo.engine.*" %>
  <%@ page import="tajo.util.TajoIdUtils" %>
  <%@ page import="java.net.InetSocketAddress" %>
  <%@ page import="java.net.InetAddress"  %>
  <%@ page import="java.text.SimpleDateFormat" %>
  <%@ page import="java.text.DecimalFormat" %>
  <%@ page import="org.apache.hadoop.conf.Configuration" %>


  <%@include file="./header.jsp" %>

  <%
    SimpleDateFormat df = new SimpleDateFormat("HH:mm:ss");
    String queryIdStr = request.getParameter("qid");
    QueryId qid = TajoIdUtils.createQueryId(queryIdStr);
    QueryMaster qm = master.getContext().getQuery(qid);
    float progress = qm.getContext().getProgress()*100;
    long startTime = qm.getContext().getStartTime();
    long finishTime = qm.getContext().getFinishTime();
    TajoProtos.QueryState finalState = qm.getContext().getQuery().getState();
    String responseTime;
    if (finalState == TajoProtos.QueryState.QUERY_SUCCEEDED) {
      responseTime = ((finishTime - startTime) / 1000) + " sec";
    } else {
      responseTime = "Nil";
    }
    class SubQueryInfo {
      SubQuery subQuery;
      String parentId;
      int px;
      int py;
      int pos; // 0: mid 1: left 2: right
      public SubQueryInfo(SubQuery subQuery, String parentId, int px, int py, int pos) {
        this.subQuery = subQuery;
        this.parentId = parentId;
        this.px = px;
        this.py = py;
        this.pos = pos;
      }
    }
   %>

  <% if (qm == null) { %>
  <h2>No Such Query Id: <%=queryIdStr%></h2>
  <% } else { %>

  <div class ="container-tajo">

  <table>
  <tr><th colspan="6">Running Query Detail: <%=queryIdStr%></th></tr>
  <tr>
      <th>Progress</th>
      <th>Start Time</th>
      <th>Finish Time</th>
      <th>Response Time</th>
      <th>Final State</th>
    </tr>
    <tr>
      <td><%=new DecimalFormat("##.##").format(progress)%>%</td>
      <td><%=df.format(startTime)%></td>
      <td><%=finalState == TajoProtos.QueryState.QUERY_SUCCEEDED ? df.format(finishTime) : "Nil"%></td>
      <td><%=responseTime%></td>
      <td><%=finalState%></td>
    </tr>
  </table>
  <% } %>

  <script type='text/javascript' src='js/jquery.jsPlumb-1.3.16-all-min.js'></script>

  <!--
  <script type="text/javascript" src="js/queryplan.js"></script>
  -->

  <script type='text/javascript'>
    jsPlumb.setRenderMode(jsPlumb.CANVAS);
    var i = 0;
  </script>

  <div id="demo">
    <div>
      <h3>Distributed Query Execution Plan</h3>
    </div>
    <div>
    <pre>
      <h4>SubQuery State:</h4>
      <span class="textborder" style="color:black">NEW</span><br/>
      <span class="textborder" style="color:gray">CONTAINER_ALLOCATED</span><br/>
      <span class="textborder" style="color:skyblue">INIT</span><br/>
      <span class="textborder" style="color:blue">RUNNING</span><br/>
      <span class="textborder" style="color:green">SUCCEEDED</span><br/>
      <span class="textborder" style="color:red">FAILED</span>
    </pre>
    </div>

    <!-- draw the query plan -->
    <%
    String curIdStr = null;
    int x=35, y=-10;
    int pos;
    List<SubQueryInfo> q = new ArrayList<SubQueryInfo>();
    Query query = qm.getContext().getQuery();
    q.add(new SubQueryInfo(query.getPlan().getRoot(), null, x, y, 0));
    while (!q.isEmpty()) {
      SubQueryInfo cur = q.remove(0);
      curIdStr = cur.subQuery.getId().toString();
      y = cur.py + 15;
      if (cur.pos == 0) {
        x = cur.px;
      } else if (cur.pos == 1) {
        x = cur.px - 20;
      } else if (cur.pos == 2) {
        x = cur.px + 20;
      }
      %>

      <div class="component window" id="<%=curIdStr%>" style="left:<%=x%>em;top:<%=y%>em;">
        <p><b>SubQuery</b> (<%=new DecimalFormat("##.##").format(cur.subQuery.getProgress() * 100)%>%)</p><br/>
        <p><a style="font-size:0.8em;" href="./subqueryinfo.jsp?subqid=<%=curIdStr%>"><%=curIdStr%></a></p>
      </div>

      <% if (cur.parentId != null) { %>
        <script type="text/javascript">
          var src = window.jsPlumb.addEndpoint(
            "<%=curIdStr%>",
            {
              anchor:"AutoDefault",
              paintStyle:{
                fillStyle:"CornflowerBlue "
              },
              hoverPaintStyle:{
                fillStyle:"red"
              }
            }
          );

          var dst = jsPlumb.addEndpoint(
            "<%=cur.parentId%>",
            {
              anchor:"AutoDefault",
              paintStyle:{
                fillStyle:"CornflowerBlue "
              },
              hoverPaintStyle:{
                fillStyle:"red"
              }
            }
          );

          var con = jsPlumb.connect({
            source:src,
            target:dst,
            paintStyle:{ strokeStyle:"CornflowerBlue ", lineWidth:4  },
            hoverPaintStyle:{ strokeStyle:"red", lineWidth:7 },

            overlays : [ <!-- overlays start -->
              ["Label", {
                cssClass:"l1 component label",
                label : "<%=cur.subQuery.getOutputType()%>",
                location:0.5,
                id:"lable",
                events:{
                  "click":function(label, evt) {
                    alert("clicked on label for connection " + label.component.id);
                  }
                }
              }], <!-- label end -->
            ] <!-- overlays end -->
          });
        </script>
      <% } %>

      <script type='text/javascript'>
        var e = document.getElementById("<%=curIdStr%>");
        var state = "<%=cur.subQuery.getState().name()%>";

        switch (state) {
          case 'NEW':
            e.style.borderColor = "black";
            e.style.color = "black";
            break;
          case 'CONTAINER_ALLOCATED':
            e.style.borderColor = "gray";
            e.style.color = "gray";
            break;
          case 'INIT':
            e.style.borderColor = "skyblue";
            e.style.color = "skyblue";
            break;
          case 'RUNNING':
            e.style.borderColor = "blue";
            e.style.color = "blue";
            break;
          case 'SUCCEEDED':
            e.style.borderColor = "green";
            e.style.color = "green";
            break;
          case 'FAILED':
            e.style.borderColor = "red";
            e.style.color = "red";
            break;
          default:
            break;
        }
      </script>

      <%
      Collection<SubQuery> children = cur.subQuery.getChildQueries();
      if (children.size() == 1) {
        pos = 0;
      } else {
        pos = 1;
      }
      for (SubQuery child : children) {
        q.add(new SubQueryInfo(child, curIdStr, x, y, pos++));
      }
    }
    %>

    <!-- end drawing -->
  </div>

  </div> <!-- container-tajo -->

  <script type='text/javascript'>
    function act() {

    }

  </script>
  </body>
</html>
