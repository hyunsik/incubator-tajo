select
  substr(host, strpos(host, ',') + 1 , length(host)) as str1 ,
  count(*) as pv,
  count(distinct imsi_num) as uv
from
  sdp_app_log_all
where (host like '%11st%' or host like '%gmarket%' or host like '%auction%' or host like '%coupang%')
  and (content_type is null or content_type = '' or content_type = 'text/html')
group by
  str1