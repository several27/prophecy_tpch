WITH samples_tpch_nation AS (

  SELECT * 
  
  FROM {{ source('samples.tpch', 'nation') }}

),

samples_tpch_part AS (

  SELECT * 
  
  FROM {{ source('samples.tpch', 'part') }}

),

samples_tpch_partsupp AS (

  SELECT * 
  
  FROM {{ source('samples.tpch', 'partsupp') }}

),

samples_tpch_region AS (

  SELECT * 
  
  FROM {{ source('samples.tpch', 'region') }}

),

samples_tpch_supplier AS (

  SELECT * 
  
  FROM {{ source('samples.tpch', 'supplier') }}

),

join_parts_supp_supplier_nation_region AS (

  SELECT * 
  
  FROM samples_tpch_part
  INNER JOIN samples_tpch_partsupp
     ON samples_tpch_part.p_partkey = samples_tpch_partsupp.ps_partkey
  INNER JOIN samples_tpch_supplier
     ON samples_tpch_partsupp.ps_suppkey = samples_tpch_supplier.s_suppkey
  INNER JOIN samples_tpch_nation
     ON samples_tpch_supplier.s_nationkey = samples_tpch_nation.n_nationkey
  INNER JOIN samples_tpch_region AS region
     ON samples_tpch_nation.n_regionkey = region.r_regionkey

)

SELECT *

FROM join_parts_supp_supplier_nation_region
