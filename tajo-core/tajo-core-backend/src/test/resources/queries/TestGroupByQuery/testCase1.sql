select
  count(1)
from
  lineitem as l1 join nation as l2 on l1.l_orderkey = l2.n_nationkey;