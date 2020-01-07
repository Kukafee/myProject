-- 使用数据库
use infoflow;

-- 配置参数
set mapreduce.job.reduce.slowstart.completedmaps=0.85;




-- ----------------------------------------------------------------------------
-- 创建 全量 男 兴趣定向(一级&二级) 特征表 pop_interest1_2_m
-- ----------------------------------------------------------------------------
drop table if exists pop_interest1_2_m;      -- 表名
create table if not exists pop_interest1_2_m -- 表名
    (
    mid string,
    interest1_2 string                     -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into pop_interest1_2_m(mid, interest1_2)  -- 表名, 字段名
select
    distinct t2.mid, t1.interest1_2        -- 字段名
from app_interest1_2 t1                    -- 表1
inner join pop_mid_appid_m t2          -- 表2
on(t1.appid=t2.appid);


-- ----------------------------------------------------------------------------
-- 创建 全量 女 兴趣定向(一级&二级) 特征表 pop_interest1_2_w
-- ----------------------------------------------------------------------------
drop table if exists pop_interest1_2_w;      -- 表名
create table if not exists pop_interest1_2_w -- 表名
    (
    mid string,
    interest1_2 string                     -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into pop_interest1_2_w(mid, interest1_2)  -- 表名, 字段名
select
    distinct t2.mid, t1.interest1_2        -- 字段名
from app_interest1_2 t1                    -- 表1
inner join pop_mid_appid_w t2          -- 表2
on(t1.appid=t2.appid);


-- ----------------------------------------------------------------------------
-- 创建 样本 男 兴趣定向(一级&二级) 特征表 bsn_interest1_2_m
-- ----------------------------------------------------------------------------
drop table if exists bsn_interest1_2_m;      -- 表名
create table if not exists bsn_interest1_2_m -- 表名
    (
    mid string,
    interest1_2 string                     -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into bsn_interest1_2_m(mid, interest1_2)  -- 表名, 字段名
select
    distinct t2.mid, t1.interest1_2        -- 字段名
from app_interest1_2 t1                    -- 表1
inner join bsn_mid_appid_m t2          -- 表2
on(t1.appid=t2.appid);


-- ----------------------------------------------------------------------------
-- 创建 样本 女 兴趣定向(一级&二级) 特征表 bsn_interest1_2_w
-- ----------------------------------------------------------------------------
drop table if exists bsn_interest1_2_w;      -- 表名
create table if not exists bsn_interest1_2_w -- 表名
    (
    mid string,
    interest1_2 string                     -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into bsn_interest1_2_w(mid, interest1_2)  -- 表名, 字段名
select
    distinct t2.mid, t1.interest1_2        -- 字段名
from app_interest1_2 t1                    -- 表1
inner join bsn_mid_appid_w t2          -- 表2
on(t1.appid=t2.appid);

-- ***********************************************************

-- --------------------------------------------------------------
-- --------------------------------------------------------------
-- --------------------------------------------------------------

-- --------------------------------------------------------------
-- 创建 全量 男 兴趣定向(一级&二级) 分布表 pop_interest1_2_dist_m
-- --------------------------------------------------------------
drop table if exists pop_interest1_2_dist_m;      -- 表名
create table if not exists pop_interest1_2_dist_m -- 表名
    (
    interest1_2 string,                       -- 特征名
    uv int,
    pop_per float                        -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into pop_interest1_2_dist_m           -- 表名
select
    table_label.label,
    table_label.label_uv,
    table_label.label_uv / table_pop.pop_uv as rate 
from 
    (
    select interest1_2 as label, count(mid) as label_uv from pop_interest1_2_m group by interest1_2   -- 表名，字段名*2
    ) as table_label  
left join
    (
    select count(mid) as pop_uv from pop_interest1_2_m      -- 表名
    ) as table_pop
on 1=1;




-- --------------------------------------------------------------
-- 创建 全量 女 兴趣定向(一级&二级) 分布表 pop_interest1_2_dist_w
-- --------------------------------------------------------------
drop table if exists pop_interest1_2_dist_w;      -- 表名
create table if not exists pop_interest1_2_dist_w -- 表名
    (
    interest1_2 string,                       -- 特征名
    uv int,
    pop_per float                        -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into pop_interest1_2_dist_w           -- 表名
select
    table_label.label,
    table_label.label_uv,
    table_label.label_uv / table_pop.pop_uv as rate 
from 
    (
    select interest1_2 as label, count(mid) as label_uv from pop_interest1_2_w group by interest1_2   -- 表名，字段名*2
    ) as table_label  
left join
    (
    select count(mid) as pop_uv from pop_interest1_2_w      -- 表名
    ) as table_pop
on 1=1;



-- --------------------------------------------------------------
-- 创建 样本 男 兴趣定向(一级&二级) 分布表 bsn_interest1_2_dist_m
-- --------------------------------------------------------------
drop table if exists bsn_interest1_2_dist_m;      -- 表名
create table if not exists bsn_interest1_2_dist_m -- 表名
    (
    interest1_2 string,                       -- 特征名
    uv int,
    bsn_per float                        -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into bsn_interest1_2_dist_m           -- 表名
select
    table_label.label,
    table_label.label_uv,
    table_label.label_uv / table_pop.pop_uv as rate 
from 
    (
    select interest1_2 as label, count(mid) as label_uv from bsn_interest1_2_m group by interest1_2   -- 表名，字段名*2
    ) as table_label  
left join
    (
    select count(mid) as pop_uv from bsn_interest1_2_m      -- 表名
    ) as table_pop
on 1=1;



-- --------------------------------------------------------------
-- 创建 样本 女 兴趣定向(一级&二级) 分布表 bsn_interest1_2_dist_w
-- --------------------------------------------------------------
drop table if exists bsn_interest1_2_dist_w;      -- 表名
create table if not exists bsn_interest1_2_dist_w -- 表名
    (
    interest1_2 string,                       -- 特征名
    uv int,
    bsn_per float                        -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into bsn_interest1_2_dist_w           -- 表名
select
    table_label.label,
    table_label.label_uv,
    table_label.label_uv / table_pop.pop_uv as rate 
from 
    (
    select interest1_2 as label, count(mid) as label_uv from bsn_interest1_2_w group by interest1_2   -- 表名，字段名*2
    ) as table_label  
left join
    (
    select count(mid) as pop_uv from bsn_interest1_2_w     -- 表名
    ) as table_pop
on 1=1;

-- ***********************************************************


-- --------------------------------------------------------------
-- 创建 男性 兴趣定向(一级&二级) tgi 数据表 tgi_interest1_2_m
-- --------------------------------------------------------------
drop table if exists tgi_interest1_2_m;        -- 表名
create table if not exists tgi_interest1_2_m   -- 表名
    (
    interest1_2 string,                        -- 特征名
    tgi_value float
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into tgi_interest1_2_m    -- 表名

select 
    bsn_table.interest1_2,   -- 特征名
    cast(bsn_table.bsn_per / pop_table.pop_per as decimal(12,6)) as tgi_value
from 
    bsn_interest1_2_dist_m as bsn_table    -- 表bsn
inner join
    pop_interest1_2_dist_m as pop_table    -- 表pop
on
    bsn_table.interest1_2 = pop_table.interest1_2     -- 特征名*2
order by tgi_value desc;



-- --------------------------------------------------------------
-- 创建 女性 兴趣定向(一级&二级) tgi 数据表 tgi_interest1_2_w
-- --------------------------------------------------------------
drop table if exists tgi_interest1_2_w;        -- 表名
create table if not exists tgi_interest1_2_w   -- 表名
    (
    interest1_2 string,                        -- 特征名
    tgi_value float
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into tgi_interest1_2_w    -- 表名

select 
    bsn_table.interest1_2,   -- 特征名
    cast(bsn_table.bsn_per / pop_table.pop_per as decimal(12,6)) as tgi_value
from 
    bsn_interest1_2_dist_w as bsn_table    -- 表bsn
inner join
    pop_interest1_2_dist_w as pop_table    -- 表pop
on
    bsn_table.interest1_2 = pop_table.interest1_2     -- 特征名*2
order by tgi_value desc;










