select l_orderkey, l_partkey + 1 as "plus1", l_partkey + 1 as "plus2" from lineitem where l_orderkey > -1;