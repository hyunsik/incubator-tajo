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

  <%@ page import="tajo.conf.TajoConf" %>
  <!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
    <html>
    <head>
    <link rel="stylesheet" type = "text/css" href = "css/tajo.css" />
    <link rel="stylesheet" type="text/css" href="css/queryplan.css">

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