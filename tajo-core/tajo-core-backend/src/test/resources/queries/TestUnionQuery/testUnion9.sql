SELECT
  n_nationkey,
  n_name,
  n_regionkey

FROM (
  SELECT
    *
  FROM
    nation
  WHERE
    n_regionkey = 0


  UNION ALL

  SELECT
    *
  FROM
    nation
  WHERE
    n_regionkey = 0

) T
GROUP BY
  n_nationkey,
	n_name;