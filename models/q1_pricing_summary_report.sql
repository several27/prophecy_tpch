{{
  config({    
    "materialized": "table"
  })
}}

WITH lineitem AS (

  SELECT * 
  
  FROM {{ source('samples.tpch', 'lineitem') }}

),

shipdate_before_1998_12_01 AS (

  SELECT * 
  
  FROM lineitem
  
  WHERE /*this should be coming from a variable*/
        l_shipdate <= DATE_SUB('1998-12-01', 90)

),

summarize_shipments AS (

  SELECT 
    any_value(l_returnflag) AS l_returnflag,
    any_value(l_linestatus) AS l_linestatus,
    sum(l_quantity) AS sum_qty,
    sum(l_extendedprice) AS sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    sum(l_extendedprice * (1 - l_discount)) AS sum_charge,
    avg(l_quantity) AS avg_qty,
    avg(l_extendedprice) AS avg_price,
    avg(l_discount) AS avg_disc,
    count(*) AS count_order
  
  FROM shipdate_before_1998_12_01
  
  GROUP BY 
    l_returnflag, l_linestatus

),

order_by_returnflag_linestatus AS (

  SELECT * 
  
  FROM summarize_shipments
  
  ORDER BY l_returnflag ASC, l_linestatus ASC

)

SELECT *

FROM order_by_returnflag_linestatus
