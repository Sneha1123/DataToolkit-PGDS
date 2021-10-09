sudo initctl list

ls

hadoop fs -ls

hadoop fs -mkdir /hive_cs
 
hadoop fs -ls /

hadoop  distcp   s3n://snhivecs/2019-Nov.csv  /hive_cs/2019-Nov.csv

hadoop fs -ls /hive_cs


hadoop distcp s3n://snhivecs/2019-Oct.csv /hive_cs/2019-Oct.csv


hadoop fs -cat /hive_cs/2019-Oct.csv |head -10

hive

create database if not exists ecom comment "Database for ecommerce events";
describe database extended ecom;
set hive.cli.print.header=true;
CREATE EXTERNAL TABLE IF NOT EXISTS ecom_events (
event_time timestamp, 
event_type string,
product_id string,
category_id string,
category_code string,
brand string,
price float,
user_id bigint,
user_session string
 )
COMMENT 'ecom_events Table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION '/hive_cs'; 


ALTER TABLE ecom_events SET TBLPROPERTIES ("skip.header.line.count"="1");


ALTER TABLE ecom_events SET TBLPROPERTIES ("timestamp.formats"="yyyy-MM-dd HH:mm:ss 'UTC'");


CREATE EXTERNAL TABLE IF NOT EXISTS ecom_events_type (
event_time timestamp, 
product_id string,
category_id string,
category_code string,
brand string,
price float,
user_id bigint,
user_session string
 )
partitioned by (event_type string) clustered by (user_id) into 50 buckets
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION '/hive_cs/partitions'; 

set hive.exec.dynamic.partition=true;


set hive.exec.dynamic.partition.mode=nonstrict;


insert into table ecom_events_type partition(event_type)
select event_time, 
product_id,
category_id,
category_code,
brand,
price,
user_id,
user_session,
event_type
from ecom_events;




set hive.cli.print.header=true;


select round(sum(price),2) as total_rev_Oct from ecom_events_type where event_type='purchase' and month(event_time)=10;


select round(sum(price),2) as revenue, month(event_time) as month from ecom_events_type where event_type='purchase' group by month(event_time);


select distinct(category_code) from ecom_events where category_code!= "" and category_code is not null;


select count(product_id) as product_cnt, category_code from ecom_events where category_code != "" and category_code is not null group by category_code order by product_cnt desc;


select round(sum(price),2) as sales, brand from ecom_events_type where brand != "" and brand is not null and event_type='purchase' group by brand order by sales desc limit 1;


select round(sum(price),2) as sales, user_id from ecom_events_type where user_id is not null and event_type='purchase' group by user_id order by sales desc limit 10;


show partitions ecom_events_type;


select count(*) from ecom_events_type where event_type = "purchase";


select brand, Nov_price-Oct_price as Increase from 
(select brand,round(sum(case when month(event_time) = 10 then price end),2) as Oct_price,
round(sum(case when month(event_time) = 11 then price end),0) as Nov_price
from ecom_events_type
where brand != "" and brand is not null and event_type='purchase' 
group by brand;)where Nov_price-Oct_price>0 order by Increase desc;


select brand from 
(select brand,round(sum(case when month(event_time) = 10 then price end),2) as Oct_price,
round(sum(case when month(event_time) = 11 then price end),0) as Nov_price
from ecom_events_type
where brand != "" and brand is not null and event_type='purchase' 
group by brand);


select brand,
round((sum(case when month(event_time) = 11 then price end)- sum(case when month(event_time) = 10 then price end)),2) as Increase
from ecom_events_type
where brand != "" and brand is not null and event_type='purchase' 
group by brand
having Increase > 0
order by Increase desc;


select brand, round((sum(case when month(event_time) = 11 then price end) - sum(case when month(event_time) = 10 then price end)),2)as Revenue_Change
from ecom_events_type where event_type='purchase' and (brand != "" and brand is not null) group by brand having Revenue_Change>0;



select brand, Revenue_Change from (select brand, round((sum(case when month(event_time) = 11 then price end) - sum(case when month(event_time) = 10 then price end)),2)as Revenue_Change
from ecom_events_type where event_type='purchase' and (brand != "" and brand is not null) group by brand)x where Revenue_Change > 0;



select distinct(category) from (select distinct(split(category_code,'\\.')[0]) as category from ecom_events where category_code!= "" and category_code is not null) x;
select distinct(split(category_code,'\\.')[0]) as category from ecom_events where category_code!= "" and category_code is not null;

select count(product_id) as product_cnt, split(category_code,'\\.')[0] as category from ecom_events where category_code != "" and category_code is not null group by split(category_code,'\\.')[0] order by product_cnt desc;


select user_id as top10_users from (select round(sum(price),2) as sales, user_id from ecom_events_type where user_id is not null and event_type='purchase' group by user_id order by sales desc limit 10)x;


CREATE EXTERNAL TABLE IF NOT EXISTS ecom_events_type (
event_time timestamp, 
product_id string,
category_id string,
category_code string,
brand string,
price float,
user_id bigint,
user_session string
 )
partitioned by (event_type string) clustered by (user_id) into 50 buckets
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION '/hive_cs/partitions'; 



select brand from (select brand, round((sum(case when month(event_time) = 11 then price else 0 end) - sum(case when month(event_time) = 10 then price else 0end)),2)as Revenue_Change
from ecom_events_type where event_type='purchase' and (brand != "" and brand is not null) group by brand)x where Revenue_Change > 0;
