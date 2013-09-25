INSERT OVERWRITE INTO lineitem (l_orderkey, l_partkey, l_shipdate, l_comment)

SELECT col1,
       col2,
       col3,
       query
FROM (

SELECT l_orderkey as col1,
       l_partkey as col2,
       l_shipdate as col3,
      '코카콜라' as query,
      l_receiptdate
    FROM lineitem
    WHERE l_shipdate = '20130715'
    AND l_comment like '%.bong.co.kr'

UNION ALL

SELECT l_orderkey as col1,
       l_partkey as col2,
       l_shipdate as col3,
      '펩시' as query,
      l_receiptdate
  FROM lineitem
  WHERE l_shipdate = '20130714' AND l_comment like '%.bong.co.kr'

UNION ALL

SELECT l_orderkey as col1,
       l_partkey as col2,
       l_shipdate as col3,
      '펩시' as query,
      l_receiptdate
  FROM lineitem
  WHERE l_shipdate = '20130713' AND l_comment like '%.bong.co.kr'
) result