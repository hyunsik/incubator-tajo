SELECT
  *
FROM (
  SELECT
    n_nationkey,
    n_name

  FROM (
    SELECT
      n_nationkey,
      n_name
    FROM
      nation
    WHERE
      n_regionkey = 0


    UNION ALL

    SELECT
      n_nationkey,
      n_name
    FROM
      nation
    WHERE
      n_regionkey = 0

  ) T1
  GROUP BY
    n_nationkey,
    n_name
  ORDER BY
    n_nationkey desc,
    n_name desc

 UNION

 SELECT
  n_nationkey,
  n_name

 FROM (
    SELECT
      n_nationkey,
      n_name
    FROM
      nation
    WHERE
      n_regionkey = 0

    UNION ALL

    SELECT
      n_nationkey,
      n_name
    FROM
      nation
    WHERE
      n_regionkey = 0
 ) T2

 GROUP BY
   n_nationkey,
   n_name

 ORDER BY
   n_nationkey desc,
   n_name desc

) TABLE1

ORDER BY
  n_nationkey desc

UNION

SELECT
  *
FROM (
  SELECT
    n_nationkey,
    n_name

  FROM (
    SELECT
      n_nationkey,
      n_name
    FROM
      nation
    WHERE
      n_regionkey = 0


    UNION ALL

    SELECT
      n_nationkey,
      n_name
    FROM
      nation
    WHERE
      n_regionkey = 0

  ) T1
  GROUP BY
    n_nationkey,
    n_name
  ORDER BY
    n_nationkey desc,
    n_name desc

 UNION

 SELECT
  n_nationkey,
  n_name

 FROM (
    SELECT
      n_nationkey,
      n_name
    FROM
      nation
    WHERE
      n_regionkey = 0

    UNION ALL

    SELECT
      n_nationkey,
      n_name
    FROM
      nation
    WHERE
      n_regionkey = 0
 ) T2

 GROUP BY
   n_nationkey,
   n_name

 ORDER BY
   n_nationkey desc,
   n_name desc

) TABLE2

ORDER BY
  n_nationkey;