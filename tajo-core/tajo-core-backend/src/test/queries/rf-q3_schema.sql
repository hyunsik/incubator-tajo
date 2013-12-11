CREATE EXTERNAL TABLE sdp_app_log_all (
  host text,
  imsi_num text,
  content_type text
) USING CSV LOCATION 'file:///home/hyunsik/test.dat';