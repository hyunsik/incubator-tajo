create EXTERNAL table filter_weblog_dt_20130816 (
  device_name text,
  host_nickname text,
  content_type text
) USING CSV LOCATION 'file:///home/hyunsik/test2.dat';