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

  <script type="text/javascript">
    function fill_Q1() {
      var volume = document.getElementById("volume").value;
      document.getElementById("sql").value =
        "DROP TABLE q1_pricing_summary_report;\n\n"+
        "CREATE TABLE q1_pricing_summary_report ( L_RETURNFLAG STRING, L_LINESTATUS STRING, SUM_QTY DOUBLE, SUM_BASE_PRICE DOUBLE, SUM_DISC_PRICE DOUBLE, SUM_CHARGE DOUBLE, AVE_QTY DOUBLE, AVE_PRICE DOUBLE, AVE_DISC DOUBLE, COUNT_ORDER INT);\n\n"+
        "SELECT\n" +
        "   L_RETURNFLAG, L_LINESTATUS, SUM(L_QUANTITY), SUM(L_EXTENDEDPRICE), SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)), SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)*(1+L_TAX)), AVG(L_QUANTITY), AVG(L_EXTENDEDPRICE), AVG(L_DISCOUNT), COUNT(1)\n"+
        " FROM\n"+
        "   lineitem"+volume+"\n"+
        " WHERE\n"+
        "   L_SHIPDATE<='1998-09-02'\n"+
        " GROUP BY L_RETURNFLAG, L_LINESTATUS\n"+
        " ORDER BY L_RETURNFLAG, L_LINESTATUS;";
    }

    function fill_Q2() {
      var volume = document.getElementById("volume").value;
      document.getElementById("sql").value =
      "DROP TABLE q2_minimum_cost_supplier_tmp1;\n"+
      "DROP TABLE q2_minimum_cost_supplier_tmp2;\n"+
      "DROP TABLE q2_minimum_cost_supplier;\n\n"+
      "create table q2_minimum_cost_supplier_tmp1 (s_acctbal double, s_name string, n_name string, p_partkey int, ps_supplycost double, p_mfgr string, s_address string, s_phone string, s_comment string);\n"+
      "create table q2_minimum_cost_supplier_tmp2 (p_partkey int, ps_min_supplycost double);\n"+
      "create table q2_minimum_cost_supplier (s_acctbal double, s_name string, n_name string, p_partkey int, p_mfgr string, s_address string, s_phone string, s_comment string);\n"+
      "insert overwrite table q2_minimum_cost_supplier_tmp1\n"+
      "select\n"+
      "   s.s_acctbal, s.s_name, n.n_name, p.p_partkey, ps.ps_supplycost, p.p_mfgr, s.s_address, s.s_phone, s.s_comment\n"+
      " from\n"+
      "   nation"+volume+" n join region"+volume+" r\n"+
      "   on\n"+
      "     n.n_regionkey = r.r_regionkey and r.r_name = 'EUROPE'\n"+
      " join supplier"+volume+" s \n"+
      "   on\n"+
      " s.s_nationkey = n.n_nationkey\n"+
      "   join partsupp"+volume+" ps\n"+
      "  on\n"+
      " s.s_suppkey = ps.ps_suppkey\n"+
      " join part"+volume+" p\n"+
      "   on\n"+
      "   p.p_partkey = ps.ps_partkey and p.p_size = 15 and p.p_type like '%BRASS' ;\n\n"+
      " insert overwrite table q2_minimum_cost_supplier_tmp2\n"+
      " select\n"+
      "   p_partkey, min(ps_supplycost)\n"+
      " from \n"+
      "   q2_minimum_cost_supplier_tmp1\n"+
      " group by p_partkey;\n\n"+
      " insert overwrite table q2_minimum_cost_supplier\n"+
      " select\n"+
      "    t1.s_acctbal, t1.s_name, t1.n_name, t1.p_partkey, t1.p_mfgr, t1.s_address, t1.s_phone, t1.s_comment\n"+
      "  from\n"+
      "    q2_minimum_cost_supplier_tmp1 t1 join q2_minimum_cost_supplier_tmp2 t2\n"+
      "  on\n"+
      "    t1.p_partkey = t2.p_partkey and t1.ps_supplycost=t2.ps_min_supplycost\n"+
      "  order by s_acctbal desc, n_name, s_name, p_partkey\n"+
      "  limit 100;";
    }

    function fill_Q3() {
      var volume = document.getElementById("volume").value;
      document.getElementById("sql").value =
      "create table q3_shipping_priority (l_orderkey int, revenue double, o_orderdate string, o_shippriority int);\n"+
      "\nset mapred.min.split.size=536870912;\n"+
      "set hive.exec.reducers.bytes.per.reducer=1024000000;\n"+

      "Insert overwrite table q3_shipping_priority\n"+
      " select\n"+
      "   l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue, o_orderdate, o_shippriority\n"+
      " from\n"+
      "   customer"+volume+" c join orders"+volume+" o\n"+
      "     on c.c_mktsegment = 'BUILDING' and c.c_custkey = o.o_custkey\n"+
      "   join lineitem"+volume+" l\n"+
      "     on l.l_orderkey = o.o_orderkey\n"+
      " where\n"+
      "   o_orderdate < '1995-03-15' and l_shipdate > '1995-03-15'\n"+
      " group by l_orderkey, o_orderdate, o_shippriority\n"+
      " order by revenue desc, o_orderdate\n"+
      " limit 10;";
    }

    function fill_Q6() {
      var volume = document.getElementById("volume").value;
      document.getElementById("sql").value =
      " select\n"+
      "   sum(l_extendedprice*l_discount) as revenue\n"+
      " from\n"+
      "   lineitem"+volume+"\n"+
      " where\n"+
      "   l_shipdate >= '1994-01-01'\n"+
      "   and l_shipdate < '1995-01-01'\n"+
      "   and l_discount >= 0.05 and l_discount <= 0.07\n"+
      "   and l_quantity < 24;";
    }

    function fill_Q10() {
    var volume = document.getElementById("volume").value;
    document.getElementById("sql").value =
      "DROP TABLE q10_returned_item;\n"+
      "\ncreate table q10_returned_item (c_custkey int, c_name string, revenue double, c_acctbal string, n_name string, c_address string, c_phone string, c_comment string);\n"+
      "set mapred.min.split.size=536870912;\n"+
      "set hive.exec.reducers.bytes.per.reducer=1024000000;\n"+
      "insert overwrite table q10_returned_item\n"+
      "select\n"+
      "   c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) as revenue,\n"+
      "   c_acctbal, n_name, c_address, c_phone, c_comment\n"+
      " from\n"+
      "   customer"+volume+" c join orders"+volume+" o\n"+
      "   on\n"+
      "     c.c_custkey = o.o_custkey and o.o_orderdate >= '1993-10-01' and o.o_orderdate < '1994-01-01'\n"+
      "   join nation"+volume+" n\n"+
      "   on\n"+
      "     c.c_nationkey = n.n_nationkey\n"+
      "   join lineitem"+volume+" l\n"+
      "   on\n"+
      "      l.l_orderkey = o.o_orderkey and l.l_returnflag = 'R'\n"+
      "   group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment\n"+
      "   order by revenue desc\n"+
      "   limit 20;";
    }

    function fill_Q12() {
      var volume = document.getElementById("volume").value;
      document.getElementById("sql").value =
      "DROP TABLE q12_shipping;\n\n"+
      "create table q12_shipping(l_shipmode string, high_line_count double, low_line_count double);\n"+
      "set mapred.min.split.size=536870912;\n"+
      "set hive.exec.reducers.bytes.per.reducer=1225000000;\n"+
      "\ninsert overwrite table q12_shipping\n"+
      "   select\n"+
      "     l_shipmode,\n"+
      "     sum(case\n"+
      "       when o_orderpriority ='1-URGENT'\n"+
      "            or o_orderpriority ='2-HIGH'\n"+
      "       then 1\n"+
      "       else 0\n"+
      "   end\n"+
      "     ) as high_line_count,\n"+
      "     sum(case\n"+
      "       when o_orderpriority <> '1-URGENT'\n"+
      "            and o_orderpriority <> '2-HIGH'\n"+
      "       then 1\n"+
      "       else 0\n"+
      "   end\n"+
      "     ) as low_line_count\n"+
      "   from\n"+
      "  orders"+volume+" o join lineitem"+volume+" l\n"+
      "   on\n"+
      "     o.o_orderkey = l.l_orderkey and l.l_commitdate < l.l_receiptdate\n"+
      " and l.l_shipdate < l.l_commitdate and l.l_receiptdate >= '1994-01-01'\n"+
      " and l.l_receiptdate < '1995-01-01'\n"+
      " where\n"+
      "   l.l_shipmode = 'MAIL' or l.l_shipmode = 'SHIP'\n"+
      " group by l_shipmode\n"+
      " order by l_shipmode;";
    }

    function fill_Q14() {
    var volume = document.getElementById("volume").value;
      document.getElementById("sql").value =
      "set mapred.min.split.size=536870912;\n"+
      "set hive.exec.reducers.bytes.per.reducer=1040000000;\n\n"+
      "select\n"+
      "   100.00 * sum(case\n"+
      "                when p_type like 'PROMO%'\n"+
      "                then l_extendedprice*(1-l_discount)\n"+
      "                else 0.0\n"+
      "                end\n"+
      "   ) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue\n"+
      "from\n"+
      "   part"+volume+" p join lineitem"+volume+" l\n"+
      "   on\n"+
      "     l.l_partkey = p.p_partkey and l.l_shipdate >= '1995-09-01' and l.l_shipdate < '1995-10-01';";
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
      <div id="message"><h4>Please copy the above query into the command line interface and then submit the query.</h4></div>
	    <input id="submitbtn" type="button" value="submit" onclick="submitQuery()" />
      </form>
  </div>
  </div> <!-- container-tajo -->
  </body>
  <script type="text/javascript">
    document.getElementById("submitbtn").disabled=true;
  </script>
</html>
