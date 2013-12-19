select
  a.id,
  a.name,
  b.id as id2,
  b.name as name2,
  case when b.name is null then '9991231' else b.name end
from
  table1 a left outer join table2 b on a.id = b.id;