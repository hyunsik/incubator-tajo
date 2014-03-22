/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.benchmark;

import com.google.common.collect.Maps;
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.CatalogConstants;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.common.TajoDataTypes.Type;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

public class TPCH extends BenchmarkSet {
  private final Log LOG = LogFactory.getLog(TPCH.class);
  private final String BENCHMARK_DIR = "benchmark/tpch";

  public static final String LINEITEM = "LINEITEM";
  public static final String CUSTOMER = "CUSTOMER";
  public static final String NATION = "NATION";
  public static final String PART = "PART";
  public static final String REGION = "REGION";
  public static final String ORDERS = "ORDERS";
  public static final String PARTSUPP = "PARTSUPP";
  public static final String SUPPLIER = "SUPPLIER";
  public static final String EMPTY_ORDERS = "EMPTY_ORDERS";


  public static final Map<String, Long> tableVolumes = Maps.newHashMap();

  static {
    tableVolumes.put(LINEITEM, 759863287L);
    tableVolumes.put(CUSTOMER, 24346144L);
    tableVolumes.put(NATION, 2224L);
    tableVolumes.put(PART, 24135125L);
    tableVolumes.put(REGION, 389L);
    tableVolumes.put(ORDERS, 171952161L);
    tableVolumes.put(PARTSUPP, 118984616L);
    tableVolumes.put(SUPPLIER, 1409184L);
    tableVolumes.put(EMPTY_ORDERS, 0L);

  }

  @Override
  public void loadSchemas() {
    Schema lineitem = new Schema()
        .addColumn("l_orderkey".toUpperCase(), Type.INT4) // 0
        .addColumn("l_partkey".toUpperCase(), Type.INT4) // 1
        .addColumn("l_suppkey".toUpperCase(), Type.INT4) // 2
        .addColumn("l_linenumber".toUpperCase(), Type.INT4) // 3
        .addColumn("l_quantity".toUpperCase(), Type.FLOAT8) // 4
        .addColumn("l_extendedprice".toUpperCase(), Type.FLOAT8) // 5
        .addColumn("l_discount".toUpperCase(), Type.FLOAT8) // 6
        .addColumn("l_tax".toUpperCase(), Type.FLOAT8) // 7
            // TODO - This is temporal solution. 8 and 9 are actually Char type.
        .addColumn("l_returnflag".toUpperCase(), Type.TEXT) // 8
        .addColumn("l_linestatus".toUpperCase(), Type.TEXT) // 9
            // TODO - This is temporal solution. 10,11, and 12 are actually Date type.
        .addColumn("l_shipdate".toUpperCase(), Type.TEXT) // 10
        .addColumn("l_commitdate".toUpperCase(), Type.TEXT) // 11
        .addColumn("l_receiptdate".toUpperCase(), Type.TEXT) // 12
        .addColumn("l_shipinstruct".toUpperCase(), Type.TEXT) // 13
        .addColumn("l_shipmode".toUpperCase(), Type.TEXT) // 14
        .addColumn("l_comment".toUpperCase(), Type.TEXT); // 15
    schemas.put(LINEITEM, lineitem);

    Schema customer = new Schema()
        .addColumn("c_custkey".toUpperCase(), Type.INT4) // 0
        .addColumn("c_name".toUpperCase(), Type.TEXT) // 1
        .addColumn("c_address".toUpperCase(), Type.TEXT) // 2
        .addColumn("c_nationkey".toUpperCase(), Type.INT4) // 3
        .addColumn("c_phone".toUpperCase(), Type.TEXT) // 4
        .addColumn("c_acctbal".toUpperCase(), Type.FLOAT8) // 5
        .addColumn("c_mktsegment".toUpperCase(), Type.TEXT) // 6
        .addColumn("c_comment".toUpperCase(), Type.TEXT); // 7
    schemas.put(CUSTOMER, customer);

    Schema nation = new Schema()
        .addColumn("n_nationkey".toUpperCase(), Type.INT4) // 0
        .addColumn("n_name".toUpperCase(), Type.TEXT) // 1
        .addColumn("n_regionkey".toUpperCase(), Type.INT4) // 2
        .addColumn("n_comment".toUpperCase(), Type.TEXT); // 3
    schemas.put(NATION, nation);

    Schema part = new Schema()
        .addColumn("p_partkey".toUpperCase(), Type.INT4) // 0
        .addColumn("p_name".toUpperCase(), Type.TEXT) // 1
        .addColumn("p_mfgr".toUpperCase(), Type.TEXT) // 2
        .addColumn("p_brand".toUpperCase(), Type.TEXT) // 3
        .addColumn("p_type".toUpperCase(), Type.TEXT) // 4
        .addColumn("p_size".toUpperCase(), Type.INT4) // 5
        .addColumn("p_container".toUpperCase(), Type.TEXT) // 6
        .addColumn("p_retailprice".toUpperCase(), Type.FLOAT8) // 7
        .addColumn("p_comment".toUpperCase(), Type.TEXT); // 8
    schemas.put(PART, part);

    Schema region = new Schema()
        .addColumn("r_regionkey".toUpperCase(), Type.INT4) // 0
        .addColumn("r_name".toUpperCase(), Type.TEXT) // 1
        .addColumn("r_comment".toUpperCase(), Type.TEXT); // 2
    schemas.put(REGION, region);

    Schema orders = new Schema()
        .addColumn("o_orderkey".toUpperCase(), Type.INT4) // 0
        .addColumn("o_custkey".toUpperCase(), Type.INT4) // 1
        .addColumn("o_orderstatus".toUpperCase(), Type.TEXT) // 2
        .addColumn("o_totalprice".toUpperCase(), Type.FLOAT8) // 3
            // TODO - This is temporal solution. o_orderdate is actually Date type.
        .addColumn("o_orderdate".toUpperCase(), Type.TEXT) // 4
        .addColumn("o_orderpriority".toUpperCase(), Type.TEXT) // 5
        .addColumn("o_clerk".toUpperCase(), Type.TEXT) // 6
        .addColumn("o_shippriority".toUpperCase(), Type.INT4) // 7
        .addColumn("o_comment".toUpperCase(), Type.TEXT); // 8
    schemas.put(ORDERS, orders);
    schemas.put(EMPTY_ORDERS, orders);


    Schema partsupp = new Schema()
        .addColumn("ps_partkey".toUpperCase(), Type.INT4) // 0
        .addColumn("ps_suppkey".toUpperCase(), Type.INT4) // 1
        .addColumn("ps_availqty".toUpperCase(), Type.INT4) // 2
        .addColumn("ps_supplycost".toUpperCase(), Type.FLOAT8) // 3
        .addColumn("ps_comment".toUpperCase(), Type.TEXT); // 4
    schemas.put(PARTSUPP, partsupp);

    Schema supplier = new Schema()
        .addColumn("s_suppkey".toUpperCase(), Type.INT4) // 0
        .addColumn("s_name".toUpperCase(), Type.TEXT) // 1
        .addColumn("s_address".toUpperCase(), Type.TEXT) // 2
        .addColumn("s_nationkey".toUpperCase(), Type.INT4) // 3
        .addColumn("s_phone".toUpperCase(), Type.TEXT) // 4
        .addColumn("s_acctbal".toUpperCase(), Type.FLOAT8) // 5
        .addColumn("s_comment".toUpperCase(), Type.TEXT); // 6
    schemas.put(SUPPLIER, supplier);
  }

  public void loadOutSchema() {
    Schema q2 = new Schema()
        .addColumn("s_acctbal".toUpperCase(), Type.FLOAT8)
        .addColumn("s_name".toUpperCase(), Type.TEXT)
        .addColumn("n_name".toUpperCase(), Type.TEXT)
        .addColumn("p_partkey".toUpperCase(), Type.INT4)
        .addColumn("p_mfgr".toUpperCase(), Type.TEXT)
        .addColumn("s_address".toUpperCase(), Type.TEXT)
        .addColumn("s_phone".toUpperCase(), Type.TEXT)
        .addColumn("s_comment".toUpperCase(), Type.TEXT);
    outSchemas.put("q2", q2);
  }

  public void loadQueries() throws IOException {
    loadQueries(BENCHMARK_DIR);
  }

  public void loadTables() throws ServiceException {
    loadTable(LINEITEM);
    loadTable(CUSTOMER);
    loadTable(NATION);
    loadTable(PART);
    loadTable(REGION);
    loadTable(ORDERS);
    loadTable(PARTSUPP) ;
    loadTable(SUPPLIER);
    loadTable(EMPTY_ORDERS);

  }

  private void loadTable(String tableName) throws ServiceException {
    TableMeta meta = CatalogUtil.newTableMeta(StoreType.CSV);
    meta.putOption(CatalogConstants.CSVFILE_DELIMITER, CatalogConstants.CSVFILE_DELIMITER_DEFAULT);

    try {
      tajo.createExternalTable(tableName, getSchema(tableName), new Path(dataDir, tableName), meta);
    } catch (SQLException s) {
      throw new ServiceException(s);
    }
  }
}
