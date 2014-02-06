select
  c_custkey,
  c_name
from
  customer_parts, nation
where
  c_nationkey = n_nationkey;