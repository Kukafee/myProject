-- 
-- tgi测试
drop table if exists tgi_behavior;   -- --------------------------------表名
create table if not exists tgi_behavior(   -- --------------------------表名
    APPbehavior string,   -- --------------------------------标签名
    tgi_value double)
row format delimited
fields terminated by ',';
-- 插入数据
insert into
    tgi_behavior   -- ---------------------------------------------------表名
select
    rate_total.feature,
    -- rate_target.feature_target / rate_total.feature_total tgi_value
    cast(rate_target.feature_target / rate_total.feature_total as decimal(12,6)) as tgi_value
from -- -
    (select
    table_target.feature as feature,
    table_target.feature_count / 349 as feature_target   -- -------------目标群体样本个数2560647
    from
    (select behavior feature, count(distinct(mid)) feature_count   -- -----字段名
    from pangxk.mid_behavior_p    -- -------------------------------------选择数据库和表
    group by behavior) as table_target) as rate_target   -- ---------------字段名
join -- -
    (select
    table_total.feature as feature,
    table_total.feature_count / 2560647 as feature_total   -- ------------总体样本个数2560647
    from
    (select behavior feature, count(distinct(mid)) feature_count   -- ------字段名
    from pangxk.mid_behavior    -- ---------------------------------------选择数据库和表
    group by behavior) as table_total)as rate_total   -- -------------------字段名
on -- -
    rate_target.feature=rate_total.feature
order by tgi_value desc;

-- --------------------------------------------------------------------------------------------
drop table if exists tgi_behavior;   -- --------------------------------表名
create table if not exists tgi_behavior(   -- --------------------------表名
    APPbehavior string,   -- --------------------------------标签名
    tgi_value double)
row format delimited
fields terminated by ',';
-- 插入数据
insert into
    tgi_behavior   -- ---------------------------------------------------表名
select
    rate_total.feature,
    -- rate_target.feature_target / rate_total.feature_total tgi_value
    cast(rate_target.feature_target / rate_total.feature_total as decimal(12,6)) as tgi_value
from -- -
    (select
    table_target.feature as feature,
    table_target.feature_count / 349 as feature_target   -- -------------目标群体样本个数2560647
    from
    (select behavior feature, count(distinct(mid)) feature_count   -- -----字段名
    from pangxk.mid_behavior_p    -- -------------------------------------选择数据库和表
    group by behavior) as table_target) as rate_target   -- ---------------字段名
join -- -
    (select
    table_total.feature as feature,
    table_total.feature_count / 2560647 as feature_total   -- ------------总体样本个数2560647
    from
    (select behavior feature, count(distinct(mid)) feature_count   -- ------字段名
    from pangxk.mid_behavior    -- ---------------------------------------选择数据库和表
    group by behavior) as table_total)as rate_total   -- -------------------字段名
on -- -
    rate_target.feature=rate_total.feature
order by tgi_value desc;

-- ----------------------------------------------------------------------------------------

drop view if exists pangxk.behavior_target_view;
create view if not exists pangxk.behavior_target_view as
select
target_table.feature,
target_table.feature_num / target_table.total_num as rate_target
from
(
select
feature_num.behavior feature,
feature_num.feature_num feature_num,
total_num.total_num total_num
from
(select behavior feature, count(distinct(mid)) feature_num from pangxk.mid_behavior_p group by feature) as feature_num
left join
(select behavior feature, count(distinct(mind)) total_num from pangxk.mid_behavior_p) as total_num
on 1=1
) as target_table
group by feature;




select count(distinct mid) from mid_behavior; 

select count(distinct mid) from mid_article;
select count(mid) from (select distinct mid as mid from mid_behavior);


from edw.test_tablename where dt = '${dt}';

set hive.exec.reducers.bytes.per.reducer
set hive.exec.reducers.max
set mapred.reduce.tasks = 15;
set mapred.reduce.tasks= 200;

create table mid_article_(

mid string)


-- -----------------------------------------------
use pangxk;
drop table if exists mid_behavior_;   -- 表名
create table if not exists mid_behavior_   -- 表名
    (
    mid string     -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
insert into table mid_behavior_
select distinct mid from mid_behavior
select count(mid) from mid_behavior_; 

drop table if exists mid_behavior_;

-- -----------------------------------------------








select 
behavior as feature, 

from
(
select


left join
(select behavior, count(distinct()))

)
group by feature; 















-- select distinct mid, behavior from pangxk.mid_behavior_p

-- select count(distinct mid) from pangxk.mid_behavior_p
-- select count(distinct mid, behavior) from pangxk.mid_behavior_p


-- select count(distinct mid) from mid_interest1;

-- select count(distinct mid) from mid_article;

-- select count(mid) from mid_behavior;

select count(distinct mid) from mid_behavior;

select count(mid) from (select distinct(mid) as mid from pangxk.mid_behavior_p);

-- select count(*) from (select distinct mid from mid_interest1);

-- select count(*) from (select distinct mid from mid_behavior)

-- select count(*) from (select distinct(mid)from mid_behavior);
INSERT OVERWRITE [LOCAL] DIRECTORY directory1 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' 
select_statement1;








