<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>

<%@ page import="org.apache.tajo.master.querymaster.*" %>
<%@ page import="java.util.*" %>
<%@ page import="java.net.InetSocketAddress" %>
<%@ page import="java.net.InetAddress"  %>
<%@ page import="org.apache.hadoop.conf.Configuration" %>
<%@ page import="org.apache.tajo.webapp.StaticHttpServer" %>
<%@ page import="org.apache.tajo.worker.*" %>
<%@ page import="org.apache.tajo.master.*" %>
<%@ page import="org.apache.tajo.master.rm.*" %>
<%@ page import="org.apache.tajo.catalog.*" %>
<%@ page import="java.text.SimpleDateFormat" %>
<%@ page import="org.apache.tajo.QueryId" %>
<%@ page import="org.apache.tajo.util.TajoIdUtils" %>
<%@ page import="org.apache.tajo.master.querymaster.*" %>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">

<%
  QueryId queryId = TajoIdUtils.parseQueryId(request.getParameter("queryId"));
  String sort = request.getParameter("sort");
  if(sort == null) {
    sort = "id";
  }
  String sortOrder = request.getParameter("sortOrder");
  if(sortOrder == null) {
    sortOrder = "asc";
  }

  String nextSortOrder = "asc";
  if("asc".equals(sortOrder)) {
    nextSortOrder = "desc";
  }

  TajoWorker tajoWorker = (TajoWorker) StaticHttpServer.getInstance().getAttribute("tajo.info.server.object");
  QueryMasterTask queryMasterTask = tajoWorker.getWorkerContext()
          .getTajoWorkerManagerService().getQueryMaster().getQueryMasterTask(queryId, true);

  Query query = queryMasterTask.getQuery();
  Collection<SubQuery> subQueries = query.getSubQueries();

  SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  String url = "querydetail.jsp?queryId=" + queryId + "&sortOrder=" + nextSortOrder + "&sort=";
  String tajoMasterHttp = "http://" + WorkerJSPUtil.getTajoMasterHttpAddr(tajoWorker.getConfig());
%>
<html>
<head>
  <link rel="stylesheet" type="text/css" href="/static/style.css"/>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
  <title>Query Detail Info</title>
</head>
<body>
<a href='<%=tajoMasterHttp%>'><img src='/static/img/tajo_logo.png'/></a>

<h3><a href='index.jsp'><%=tajoWorker.getWorkerContext().getWorkerName()%></a></h3>
<hr/>
<h1><% out.write(queryId.toString()); %></h1>
<h2>Logical Plan</h2>
<pre>
<%
  out.write(query.getPlan().getLogicalPlan().toString());
%>
</pre>
<h2>Distributed Query Plan</h2>
<pre>
<%
  out.write(query.getPlan().toString());
%>
</pre>
<hr/>
<%
for(SubQuery eachSubQuery: subQueries) {
%>
  <div><%=eachSubQuery.getId()%>(<%=eachSubQuery.getState()%>)</div>
  <div>Started:<%=df.format(eachSubQuery.getStartTime())%>, <%=eachSubQuery.getFinishTime() == 0 ? "-" : df.format(eachSubQuery.getFinishTime())%></div>
  <table>
    <tr><th><a href='<%=url%>id'>Id</a></th><th>Status</th><th><a href='<%=url%>startTime'>Start Time</a></th><th><a href='<%=url%>runTime'>Running Time</a></th><th><a href='<%=url%>host'>Host</a></th></tr>
<%
    QueryUnit[] queryUnits = eachSubQuery.getQueryUnits();
    WorkerJSPUtil.sortQueryUnit(queryUnits, sort, sortOrder);
    for(QueryUnit eachQueryUnit: queryUnits) {
%>
      <tr>
        <td><%=eachQueryUnit.getId()%></td>
        <td><%=eachQueryUnit.getState()%></td>
        <td><%=eachQueryUnit.getLaunchTime() == 0 ? "-" : df.format(eachQueryUnit.getLaunchTime())%></td>
        <td><%=eachQueryUnit.getLaunchTime() == 0 ? "-" : eachQueryUnit.getRunningTime() + " ms"%></td>
        <td><%=eachQueryUnit.getSucceededHost() == null ? "-" : eachQueryUnit.getSucceededHost()%></td>
      </tr>
<%
    }
%>
  </table>
<%
  }
%>
</body>
</html>