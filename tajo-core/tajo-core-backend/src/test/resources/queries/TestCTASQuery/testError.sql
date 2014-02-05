create table
  lineitem_partition
partition by column (l_returnflag text, l_linestatus text)

as
  select
    l_orderkey,
    l_partkey,
    l_suppkey,
    l_linenumber,
    l_quantity,
    l_extendedprice,
    l_discount,
    l_tax,
    l_shipdate,
    l_commitdate,
    l_receiptdate,
    l_shipinstruct,
    l_shipmode,
    l_comment,
    l_returnflag,
    l_linestatus
  from
    lineitem
;