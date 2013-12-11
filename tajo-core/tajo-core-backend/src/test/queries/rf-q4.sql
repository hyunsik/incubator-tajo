select
  a.device_name
from (

  select
    LOWER(host_nickname) as host_nname,
    device_name,
    count(*) as pv

  from
    filter_weblog_dt_20130816

  where
    content_type = 'text/html' and
    host_nickname is not null and
    length(host_nickname) > 0 and
    device_name is not null and
    length(device_name) > 0

  group by
    host_nname,
    device_name

) a left outer join (

  select
    LOWER(host_nickname) as host_nname,
    device_name,
    count(*) as pv

  from
    filter_weblog_dt_20130816

  where content_type = 'text/html' and
    host_nickname is not null and
    length(host_nickname) > 0 and
    device_name is not null and
    length(device_name) > 0

  group by
    host_nname,
    device_name

) b on (a.host_nname = b.host_nname and a.device_name = b.device_name);