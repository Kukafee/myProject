
-- ----------------------------------------------------------------------------
-- 建表：生成总体的特征表，包括用户静态表（mid_ttlabel）和 用户app动态表（mid_app_label）
-- ----------------------------------------------------------------------------

-- ----------------------------------------------------------------------------
-- 新建库 pangxk_1
-- ----------------------------------------------------------------------------
drop database if exists pangxk_1;
create database if not exists pangxk_1;
use pangxk_1;
select current_database();

-- ----------------------------------------------------------------------------
-- 新建性别映射表 sex_ttsex : 本地性别标签和头条性别标签映射
-- ----------------------------------------------------------------------------
drop table if exists sex_ttsex;
create table if not exists sex_ttsex
	(
    sex int,
    ttsex string
    )
row format delimited
fields terminated by ',';
-- 加载本地数据到 sex_ttsex 数据表
load data local inpath '/root/pangxk/sex_ttsex.csv' overwrite into table sex_ttsex;






















