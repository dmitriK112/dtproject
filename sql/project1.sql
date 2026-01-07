-- connecting to warehouse
use warehouse gator_wh;
-- connecting to database
use database gator_db;
-- creating schema for project
create schema lab_project1;
use schema lab_project1;

select * from snowflake_sample_data.tpch_sf10.lineitem limit 10;
-- creating staging for lineitem table
create table lineitem_staging as
select * from snowflake_sample_data.tpch_sf10.lineitem;
-- controlling data
select * from lineitem_staging limit 10;

-- creating a view to union all dates in lineitem_staging
create or replace view all_dates as  
select l_shipdate as event_date from lineitem_staging where l_shipdate is not null
union all
select l_commitdate from lineitem_staging where l_commitdate is not null
union all
select l_receiptdate from lineitem_staging where l_receiptdate is not null;
select * from all_dates limit 10;

-- can use either another view to choose distinct dates
create or replace view distinct_dates as
select distinct event_date from all_dates;
select count(*) from distinct_dates;


-- or do it right in dim_date table
-- scd type 0
create or replace table dim_date as
select distinct 
to_char(date(d.event_date), 'YYYYMMDD') as idDate,
date(d.event_date) as date,
year(d.event_date) as year,
month(d.event_date) as month,
day(d.event_date) as day,
dayname(d.event_date) as weekday
from distinct_dates d;
select count(*) from dim_date;

-- creating dimension table for information about shipping
-- scd type 0
create or replace table dim_shipping_info as
select 
-- using window function to assign id for each unique combination
row_number() over (order by  l_shipmode,
l_shipinstruct, l_linestatus, l_returnflag) as info_id,
-- 
l_shipmode as shipmode, 
l_shipinstruct as shipinstruct, 
l_linestatus as linestatus, 
l_returnflag as returnflag
from(
select distinct l_shipmode, l_shipinstruct, l_linestatus, l_returnflag
from lineitem_staging
); 
select count(*) from dim_shipping_info limit 10;

-- creating staging for supplier with region and nation
create table supplier_staging as
select * from snowflake_sample_data.tpch_sf10.supplier s
join snowflake_sample_data.tpch_sf10.nation n 
on s.s_nationkey = n.n_nationkey
join snowflake_sample_data.tpch_sf10.region r
on n.n_regionkey = r.r_regionkey;
select count(*) from supplier_staging;
select * from supplier_staging limit 10;

-- creating staging for customer with region and nation
create table customer_staging as
select * from snowflake_sample_data.tpch_sf10.customer c
join snowflake_sample_data.tpch_sf10.nation n 
on c.c_nationkey = n.n_nationkey
join snowflake_sample_data.tpch_sf10.region r
on n.n_regionkey = r.r_regionkey;
select * from customer_staging limit 10;

-- creating staging for parts
create table part_staging as
select * from snowflake_sample_data.tpch_sf10.part;
select * from part_staging limit 10;

-- dimension table for supplier
--scd type 1
create or replace table dim_supplier as
select
s_suppkey as suppkey,
s_name as name,
s_address as address,
s_phone as phone,
s_acctbal as acctbal,
n_name as nation,
r_name as region
from supplier_staging;
select * from dim_supplier limit 10;

-- dimension table for customer
-- scd type 2
create or replace table dim_customer as
select
-- adding customer_id, because custkey is not unique in scd type 2
row_number() over(order by c_custkey) as customer_id,
c_custkey as custkey,
c_name as name,
c_address as address,
c_phone as phone,
c_acctbal as acctbal,
c_mktsegment as market_segment,
n_name as nation,
r_name as region,
-- adding start_date, end_date and is_active to transform table into scd type 2
to_date('1900-01-01') as start_date,
to_date(null) as end_date,
true as is_active
from customer_staging;
select * from dim_customer limit 10;

select * from part_staging limit 10;
-- creating dimension table for parts
create or replace table dim_part as
select
p_partkey as partkey,
p_name as name,
p_mfgr as manufacturer,
p_brand as brand,
p_type as type,
p_size as size,
p_container as container,
p_retailprice as retailprice
from part_staging;
select * from dim_part limit 10;

select * from snowflake_sample_data.tpch_sf10.orders limit 10;

-- creating staging for orders table
create table order_staging as
select * from snowflake_sample_data.tpch_sf10.orders;
select * from order_staging limit 10;


create or replace table fact_sales as
select
-- key for the fact row generated via window function
row_number() over(order by l.l_orderkey, l.l_linenumber) as saleskey,
-- foreign keys for other tables
l.l_orderkey as orderkey, -- originalorder key
p.partkey, -- links to dim_part
s.suppkey, -- links to dim_supplier
c.customer_id, -- links to dim_customer
-- 3 keys for dates
shipD.idDate as shipdatekey, -- date of shipment
commitD.idDate as commitdatekey, --promised delivery
receiptD.idDate as receiptdatekey, -- real delivery
-- key for unique combination of shopping_info's
ship.info_id as shipping_info_id,
-- metrics from lineitem_staging
l.l_quantity as quantity,
p.retailprice,
l.l_extendedprice as total_price,
l.l_discount as discount,
l.l_tax as tax,
-- window function to compute total amout forevery order
round(sum(l.l_extendedprice * (1-l.l_discount) * (1+l.l_tax)) over(partition by l.l_orderkey), 2)
as total_amount,
-- window function to rank positions in one order by their total price
rank() over(partition by l.l_orderkey order by l.l_extendedprice desc) as totalpricerank
from lineitem_staging l
join dim_part p
on l.l_partkey = p.partkey
join dim_supplier s
on l.l_suppkey = s.suppkey
join order_staging o
on o.o_orderkey = l.l_orderkey
-- adding to fact table only active customer records
join dim_customer c
on o.o_custkey = c.custkey
and c.is_active = TRUE
-- 
left join dim_date shipD
on shipD.date = l.l_shipdate
left join dim_date commitD
on commitD.date = l.l_commitdate
left join dim_date receiptD
on receiptd.date = l.l_receiptdate
join dim_shipping_info ship
on ship.shipmode = l.l_shipmode
and ship.shipinstruct = l.l_shipinstruct
and ship.linestatus = l.l_linestatus
and ship.returnflag = l.l_returnflag;
select * from fact_sales order by saleskey limit 10;
describe table fact_sales;

select distinct year from dim_date order by year;

-- cleaning up staging tables
drop table if exists lineitem_staging;
drop table if exists order_staging;
drop table if exists customer_staging;
drop table if exists supplier_staging;
drop table if exists part_staging;