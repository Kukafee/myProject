
use pangxk;

-- -----------------------------------------------------------
-- mid_ttsex_dist
-- mid_ttage_dist
-- mid_province_dist
-- mid_city_dist
-- mid_l_province_dist
-- mid_l_city_dist
-- -- -----------------------------------------------------------
-- mid_article_dist
-- mid_behavior_dist
-- mid_interest1_dist
-- mid_interest1_2_dist
-- -----------------------------------------------------------
-- -- -----------------------------------------------------------
-- mid_ttlabel2
-- mid_ttlabel_p
-- 
-- mid_article
-- mid_article_p
-- mid_behavior
-- mid_behavior_p
-- mid_interest1
-- mid_interest1_2
-- mid_interest1_2_p
-- mid_interest1_p
-- 
-- -- -----------------------------------------------------------
-- 总体性别分布表
drop table if exists pangxk.pop_ttsex_dist;
create table if not exists pangxk.pop_ttsex_dist( 
    feature string, -- 特征名 
    fn int,         -- 特征用户量
    pop_per double  -- 全量特征占比 
    )
row format delimited
fields terminated by ',';
-- 插入数据
insert into pop_ttsex_dist
select
feature,
feature_num.fnum / total_num.t_num as rate 
from 
(select ttsex as feature, count(distinct(mid)) as fnum from pangxk.mid_ttlabel_p group by ttsex) as feature_num
left join
(select count(distinct(mid)) as t_num from pangxk.mid_ttlabel_p) as total_num
on 1=1;


-- -- ----------------------------------------------------------------------------
-- 目标样本性别分布表
drop table if exists pangxk.bsn_ttsex_dist;
create table if not exists pangxk.bsn_ttsex_dist( 
    feature string,  
    fn int,          -- 目标样本数量
    pop_per double,  -- 总体样本中特征占比
    bsc_per double)  -- 目标样本中特征占比
row format delimited
fields terminated by ',';
-- 插入数据
insert into bsn_ttsex_dist
select
total_table.feature feature,
total_table.total_num,
cast(feature_num.f_num / total_num.t_num as decimal(12, 6)) as rate
from
(select ttsex as feature, count(distinct(mid)) as f_num from pangxk.mid_ttlabel_p group by ttsex) as feature_num
left join
(select ttsex as feature, count(distinct(mid)) as t_num from pangxk.mid_ttlabel_p) as total_num
left join
(select ttsex as feature, count(distinct(mid)) total_num from pangxk.mid_ttlabel2) as total_numbers)
on 1=1;



-- -- ----------------------------------------------------------------------------
-- -- 建表: tgi_ttsex 
-- -- ----------------------------------------------------------------------------
drop table if exists tgi_ttsex;   -- 表名 
create table if not exists tgi_ttsex   -- 表名
    (
    feature string,
    tgi_value double
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into tgi_ttsex
select
    t1.feature,
    cast(t1.bsc_per / t1.pop_per as decimal(12, 6)) as feature_tgi
from
bsn_ttsex_dist t1;





















