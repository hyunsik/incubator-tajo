select l_linenumber, l_orderkey as sortkey from lineitem order by l_linenumber, l_orderkey, (l_orderkey is null);