
 -- 使用数据库
 use pangxk;
drop table if exists ginitable;
create table if not exists ginitable
    (
    feature string,
    gini_value double
    )
row format delimited
fields terminated by ',';
-- -- 插入数据
-- insert into table ginitable values
-- (),
-- (),
-- ();
drop view if exists pangxk.label  ;
create view if not exists pangxk.sex_gini as
select
    feature as feature,
    sex_rate.rate*(1-sex_rate.rate) as sex_gini_value
from
    sex_rate;



select 'sex' as sex










-- -- -------------------------------------------------------------------------------------
-- 类目分类
--     性别
--     年龄
--     地址
--     文章分类
--     app行为
--     兴趣定向(一级)
--     兴趣定向(一级&二级)
-- -- -------------------------------------------------------------------------------------
-- 
-- 性别
-- 	建表sex_rate
drop table if exists sex_rate;
create table if not exists sex_rate
    (
    feature string,
    rate double
    )
row format delimited
fields terminated by ',';
-- 插入数据
insert into sex_rate
select
total_table.feature feature,
-- total_table.feature_num / total_table.total_num as feature_rate
cast(total_table.feature_num / total_table.total_num as decimal(12, 6)) as feature_rate
from
(
select
feature_num.feature feature,
feature_num.feature_num feature_num,
total_num.total_num total_num
from
(select ttsex feature, count(distinct(mid)) feature_num from pangxk.mid_ttlabel2 group by feature) as feature_num
left join
(select ttsex feature, count(distinct(mid)) total_num from pangxk.mid_ttlabel2) as total_num
on 1=1
)
as total_table
group by feature;


-- 	建表sex_gini
drop table if exists sex_gini;
create table if not exists sex_gini
    (
    feature string,
    gini_value double
    )
row format delimited
fields terminated by ',';
-- 插入数据
insert into sex_gini


drop view if exists pangxk.sex_gini;
create view if not exists pangxk.sex_gini as
select
    feature as feature,
    sex_rate.rate*(1-sex_rate.rate) as sex_gini_value
from
    sex_rate;




select sum(sex_gini_value) from pangxk.sex_gini











-- -- -------------------------------------------------------------------------------------
-- -- -------------------------------------------------------------------------------------
-- 
-- -- -------------------------------------------------------------------------------------
-- -- -------------------------------------------------------------------------------------
-- 
-- -- -------------------------------------------------------------------------------------
-- -- -------------------------------------------------------------------------------------
