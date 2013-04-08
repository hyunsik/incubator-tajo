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

  <div class ="container-tajo">

  <div style="display: inline;">
    <select id="volume">
      <option value="_100">100GB</option>
      <option value="_300">300GB</option>
      <option value="">1TB</option>
    </select>
  </div>

  <div style="display: inline;">
    <input type="button" value="Q1" onclick="fill_Q1();"/>
    <input type="button" value="Q2" onclick="fill_Q2();"/>
    <input type="button" value="Q3" onclick="fill_Q3();"/>
    <input type="button" value="Q6" onclick="fill_Q6();"/>
    <input type="button" value="Q10" onclick="fill_Q10();"/>
    <input type="button" value="Q12" onclick="fill_Q12();"/>
    <input type="button" value="Q14" onclick="fill_Q14();"/>
  </div>

  <div class = "command" >
      <form method="post" action="./submit_query.jsp">
	    <textarea id="sql" name="sql" rows="20" cols="88"></textarea>
	    <br />
	    <br />
	    <br />
      <div id="message"><h4>&nbsp;</h4></div>
	    <input id="submitbtn" type="button" value="submit" onclick="submitQuery()" />
      </form>
  </div>
  </div> <!-- container-tajo -->
  </body>
  <script type="text/javascript">
      function fill_Q1() {
        var volume = document.getElementById("volume").value;
        $("#message").replaceWith(" ");
        document.getElementById("submitbtn").disabled=false;
        document.getElementById("sql").value =
          "select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice*(1-l_discount)) as sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lineitem" + volume + " where l_shipdate <= '1998-09-01' group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus";
      }

      function fill_Q2() {
        var volume = document.getElementById("volume").value;
        $("#message").replaceWith("Please copy the above query to the command line interface and then submit the query.</h4>");
        document.getElementById("submitbtn").disabled=true;
        document.getElementById("sql").value =
        "drop table r2_1\n"+
        "drop table r2_2\n"+
        "create table r2_1 as select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment, ps_supplycost from region"+volume+" join nation"+volume+" on n_regionkey = r_regionkey join supplier"+volume+" on s_nationkey = n_nationkey join partsupp"+volume+" on s_suppkey = ps_suppkey join part"+volume+" on p_partkey = ps_partkey  where p_size = 15 and p_type like '%BRASS' and r_name = 'EUROPE'\n"+
              "create table r2_2 as select p_partkey, min(ps_supplycost) as min_ps_supplycost from r2_1 group by p_partkey\n"+
              "select s_acctbal, s_name, n_name, r2_1.p_partkey, p_mfgr, s_address, s_phone, s_comment from r2_1 join r2_2 on r2_1.p_partkey = r2_2.p_partkey where ps_supplycost = min_ps_supplycost order by s_acctbal, n_name, s_name, r2_1.p_partkey";

      }

      function fill_Q3() {
        var volume = document.getElementById("volume").value;
        $("#message").value = "";
        document.getElementById("submitbtn").disabled=false;
        document.getElementById("sql").value =
        "select l_orderkey,  sum(l_extendedprice*(1-l_discount)) as revenue, o_orderdate, o_shippriority from customer"+volume+", orders"+volume+", lineitem"+volume+" where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < '1995-03-15' and l_shipdate > '1995-03-15' group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate";
      }

      function fill_Q6() {
        var volume = document.getElementById("volume").value;
        $("#message").value = "";
        document.getElementById("submitbtn").disabled=false;
        document.getElementById("sql").value =
        "select sum(l_extendedprice*l_discount) as revenue from lineitem" + volume + " where l_shipdate >= '1994-01-01' and l_shipdate < '1995-01-01' and l_discount >= 0.05 and l_discount <= 0.07 and l_quantity < 24;";
      }

      function fill_Q10() {
      var volume = document.getElementById("volume").value;
      $("#message").value = "";
      document.getElementById("submitbtn").disabled=false;
      document.getElementById("sql").value =
        "select c_custkey, c_name, sum(l_extendedprice * (1-l_discount)) as revenue, c_acctbal, n_name, c_address, c_phone, c_comment from customer_100 join orders_100 on c_custkey = o_custkey join nation_100 on c_nationkey = n_nationkey join lineitem_100 on l_orderkey = o_orderkey where o_orderdate >= '1993-10-01' and o_orderdate < '1994-01-01' and l_returnflag = 'R' group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment order by revenue desc";
      }

      function fill_Q12() {
        var volume = document.getElementById("volume").value;
        $("#message").value = "";
        document.getElementById("submitbtn").disabled=false;
        document.getElementById("sql").value =
        "select " +
          "  l_shipmode," +
          "  sum(case when o_orderpriority ='1-URGENT' or o_orderpriority ='2-HIGH' then 1 else 0 end) as high_line_count,"+
          "  sum(case when o_orderpriority != '1-URGENT' and o_orderpriority != '2-HIGH' then 1 else 0 end) as low_line_count"+
          " from"+
          "  orders"+volume+","+
          "  lineitem"+volume+
          " where \n"+
          "  o_orderkey = l_orderkey and (l_shipmode = 'MAIL' or l_shipmode = 'SHIP') and "+
          "  l_commitdate < l_receiptdate and l_shipdate < l_commitdate and"+
          "  l_receiptdate >= '1994-01-01' and l_receiptdate < '1995-01-01'"+
          " group by l_shipmode"+
          " order by l_shipmode";
      }

      function fill_Q14() {
        var volume = document.getElementById("volume").value;
        $("#message").replaceWith("");
        document.getElementById("submitbtn").disabled=false;
        document.getElementById("sql").value =
        "select 100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice*(1-l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue from lineitem" + volume + ", part"+volume+" where l_partkey = p_partkey and l_shipdate >= '1995-09-01' and l_shipdate < '1995-10-01'";
      }

      function submitQuery() {
        $.ajax({
          type:"POST",
          url: "./submit_query.jsp",
          data: ({sql: document.getElementById("sql").value}),
          dataType: "text",
          success: function(returnval) {
            $("#sql").text="";
            $("#message").replaceWith("<h4>The query is submitted. See the detail of <a href='./queryinfo.jsp?qid=" + returnval + "'>"+returnval+"</a>.</h4>");
            $("#message").fadeIn("slow");
          },
          error: function(XMLHttpRequest, textStatus, errorThrown) {
            $("#sql").text="";
            $("#message").value = "<h4>" + textStatus + "</h4>";
            $("#message").fadeIn("slow");
          }
        });

        return false;
      }
    </script>
</html>
