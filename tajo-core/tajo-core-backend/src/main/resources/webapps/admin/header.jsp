<%@ page import="tajo.conf.TajoConf" %>
  <!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
    <html>
    <head>
    <link rel="stylesheet" type = "text/css" href = "./tajo.css" />
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
    <title>Tajo: A Distributed Data Warehouse on Large Cluster</title>
      <%
     TajoMaster master = (TajoMaster)application.getAttribute("tajo.master");
     CatalogService catalog = master.getCatalog();
     TajoConf conf = master.getContext().getConf();
    %>
    </head>
    <body>

    <div class="container-fluid">
    <div>
    <img src="./img/tajochar_title_small.jpg" />
    </div>
    </div>

    <div class="container-tajo">
    <div id="topbar">
    <ul>
    <li><a href="./index.jsp" class="headline">Tajo</a></li>
    <li><a href="./catalog.jsp" class="headline">Catalog</a></li>
    <li><a href="./cluster.jsp" class="headline">Cluster</a></li>
    <li><a href="./queries.jsp" class="headline">Queries</a></li>
    <li><a href="./tpch.jsp" class="headline">TPC-H Benchmark</a></li>
    </ul>
    </div>
    </div>

    <br /><br />