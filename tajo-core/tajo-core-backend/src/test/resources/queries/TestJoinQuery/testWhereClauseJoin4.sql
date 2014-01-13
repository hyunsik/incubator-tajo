select
  n_name,
  r_name,
  n_nationkey + 1,
  r_regionkey + 1
from
  nation, region
where
  n_regionkey = r_regionkey;