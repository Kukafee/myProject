
-- 使用数据库
use infoflow;

-- 配置参数
set mapreduce.job.reduce.slowstart.completedmaps=0.85;

-- --------------------------------------------------------------
-- 创建并计算特征分布表
-- --------------------------------------------------------------

-- --------------------------------------------------------------
-- 创建 全量 男 年龄 分布表 pop_age_dist_m
-- --------------------------------------------------------------
drop table if exists pop_age_dist_m;      -- 表名
create table if not exists pop_age_dist_m -- 表名
    (
    age string,                       -- 特征名
    uv int,
    pop_per float                     -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into pop_age_dist_m           -- 表名
select
    table_label.label,
    table_label.label_uv,
    table_label.label_uv / table_pop.pop_uv as rate 
from 
    (
    select ttage as label, count(mid) as label_uv from pop_age_m group by ttage   -- 表名，字段名*2
    ) as table_label  
left join
    (
    select count(mid) as pop_uv from pop_age_m      -- 表名
    ) as table_pop
on 1=1;
-- ------------------------------------------------------------
--         117391          6.874081370207124E-4
-- 24-30   62222958        0.364359854151494
-- 50+     551778          0.003231054229276645
-- 41-49   7599733         0.044501863885517884
-- 18-23   11170935        0.06541380188540409
-- 31-40   89110569        0.5218060177112867
-- 170 773 364
-- --------------------------------------------------------------
-- 创建 全量 女 年龄 分布表 pop_age_dist_w
-- --------------------------------------------------------------
drop table if exists pop_age_dist_w;      -- 表名
create table if not exists pop_age_dist_w -- 表名
    (
    age string,                       -- 特征名
    uv int,
    pop_per float                     -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into pop_age_dist_w           -- 表名
select
    table_label.label,
    table_label.label_uv,
    table_label.label_uv / table_pop.pop_uv as rate 
from 
    (
    select ttage as label, count(mid) as label_uv from pop_age_w group by ttage   -- 表名，字段名*2
    ) as table_label  
left join
    (
    select count(mid) as pop_uv from pop_age_w      -- 表名
    ) as table_pop
on 1=1;




-- --------------------------------------------------------------
-- 创建 样本 男 年龄 分布表 bsn_age_dist_m
-- --------------------------------------------------------------
drop table if exists bsn_age_dist_m;      -- 表名
create table if not exists bsn_age_dist_m -- 表名
    (
    age string,                       -- 特征名
    uv int,
    bsn_per float                     -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into bsn_age_dist_m           -- 表名
select
    table_label.label,
    table_label.label_uv,
    table_label.label_uv / table_pop.pop_uv as rate 
from 
    (
    select ttage as label, count(mid) as label_uv from bsn_age_m group by ttage   -- 表名，字段名*2
    ) as table_label  
left join
    (
    select count(mid) as pop_uv from bsn_age_m      -- 表名
    ) as table_pop
on 1=1;




-- --------------------------------------------------------------
-- 创建 样本 女 年龄 分布表 bsn_age_dist_w
-- --------------------------------------------------------------
drop table if exists bsn_age_dist_w;      -- 表名
create table if not exists bsn_age_dist_w -- 表名
    (
    age string,                       -- 特征名
    uv int,
    bsn_per float                     -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into bsn_age_dist_w 
select
    table_label.label,
    table_label.label_uv,
    table_label.label_uv / table_pop.pop_uv as rate 
from 
    (
    select ttage as label, count(mid) as label_uv from bsn_age_w group by ttage   -- 表名，字段名*2
    ) as table_label  
left join
    (
    select count(mid) as pop_uv from bsn_age_w      -- 表名
    ) as table_pop
on 1=1;




-- --------------------------------------------------------------
-- --------------------------------------------------------------
-- --------------------------------------------------------------

-- --------------------------------------------------------------
-- 创建 全量 男 文章分类 分布表 pop_article_dist_m
-- --------------------------------------------------------------
drop table if exists pop_article_dist_m;      -- 表名
create table if not exists pop_article_dist_m -- 表名
    (
    article string,                       -- 特征名
    uv int,
    pop_per float                        -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into pop_article_dist_m           -- 表名
select
    table_label.label,
    table_label.label_uv,
    table_label.label_uv / table_pop.pop_uv as rate 
from 
    (
    select article as label, count(mid) as label_uv from pop_article_m group by article   -- 表名，字段名*2
    ) as table_label  
left join
    (
    select count(mid) as pop_uv from pop_article_m      -- 表名
    ) as table_pop
on 1=1;






-- --------------------------------------------------------------
-- 创建 全量 女 文章分类 分布表 pop_article_dist_w
-- --------------------------------------------------------------
drop table if exists pop_article_dist_w;      -- 表名
create table if not exists pop_article_dist_w -- 表名
    (
    article string,                       -- 特征名
    uv int,
    pop_per float                        -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into pop_article_dist_w           -- 表名
select
    table_label.label,
    table_label.label_uv,
    table_label.label_uv / table_pop.pop_uv as rate 
from 
    (
    select article as label, count(mid) as label_uv from pop_article_w group by article   -- 表名，字段名*2
    ) as table_label  
left join
    (
    select count(mid) as pop_uv from pop_article_w      -- 表名
    ) as table_pop
on 1=1;





-- --------------------------------------------------------------
-- 创建 样本 男 文章分类 分布表 bsn_article_dist_m
-- --------------------------------------------------------------
drop table if exists bsn_article_dist_m;      -- 表名
create table if not exists bsn_article_dist_m -- 表名
    (
    article string,                       -- 特征名
    uv int,
    bsn_per float                        -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into bsn_article_dist_m           -- 表名
select
    table_label.label,
    table_label.label_uv,
    table_label.label_uv / table_pop.pop_uv as rate 
from 
    (
    select article as label, count(mid) as label_uv from bsn_article_m group by article   -- 表名，字段名*2
    ) as table_label  
left join
    (
    select count(mid) as pop_uv from bsn_article_m      -- 表名
    ) as table_pop
on 1=1;



-- --------------------------------------------------------------
-- 创建 样本 女 文章分类 分布表 bsn_article_dist_w
-- --------------------------------------------------------------
drop table if exists bsn_article_dist_w;      -- 表名
create table if not exists bsn_article_dist_w -- 表名
    (
    article string,                       -- 特征名
    uv int,
    bsn_per float                        -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into bsn_article_dist_w           -- 表名
select
    table_label.label,
    table_label.label_uv,
    table_label.label_uv / table_pop.pop_uv as rate 
from 
    (
    select article as label, count(mid) as label_uv from bsn_article_w group by article   -- 表名，字段名*2
    ) as table_label  
left join
    (
    select count(mid) as pop_uv from bsn_article_w      -- 表名
    ) as table_pop
on 1=1;



-- --------------------------------------------------------------
-- --------------------------------------------------------------
-- --------------------------------------------------------------

-- --------------------------------------------------------------
-- 创建 全量 男 app行为 分布表 pop_behavior_dist_m
-- --------------------------------------------------------------
drop table if exists pop_behavior_dist_m;      -- 表名
create table if not exists pop_behavior_dist_m -- 表名
    (
    behavior string,                       -- 特征名
    uv int,
    pop_per float                        -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into pop_behavior_dist_m           -- 表名
select
    table_label.label,
    table_label.label_uv,
    table_label.label_uv / table_pop.pop_uv as rate 
from 
    (
    select behavior as label, count(mid) as label_uv from pop_behavior_m group by behavior   -- 表名，字段名*2
    ) as table_label  
left join
    (
    select count(mid) as pop_uv from pop_behavior_m      -- 表名
    ) as table_pop
on 1=1;




-- --------------------------------------------------------------
-- 创建 全量 女 app行为 分布表 pop_behavior_dist_w
-- --------------------------------------------------------------
drop table if exists pop_behavior_dist_w;      -- 表名
create table if not exists pop_behavior_dist_w -- 表名
    (
    behavior string,                       -- 特征名
    uv int,
    pop_per float                        -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into pop_behavior_dist_w           -- 表名
select
    table_label.label,
    table_label.label_uv,
    table_label.label_uv / table_pop.pop_uv as rate 
from 
    (
    select behavior as label, count(mid) as label_uv from pop_behavior_w group by behavior   -- 表名，字段名*2
    ) as table_label  
left join
    (
    select count(mid) as pop_uv from pop_behavior_w      -- 表名
    ) as table_pop
on 1=1;


-- --------------------------------------------------------------
-- 创建 样本 男 app行为 分布表 bsn_behavior_dist_m
-- --------------------------------------------------------------
drop table if exists bsn_behavior_dist_m;      -- 表名
create table if not exists bsn_behavior_dist_m -- 表名
    (
    behavior string,                       -- 特征名
    uv int,
    bsn_per float                        -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into bsn_behavior_dist_m           -- 表名
select
    table_label.label,
    table_label.label_uv,
    table_label.label_uv / table_pop.pop_uv as rate 
from 
    (
    select behavior as label, count(mid) as label_uv from bsn_behavior_m group by behavior   -- 表名，字段名*2
    ) as table_label  
left join
    (
    select count(mid) as pop_uv from bsn_behavior_m      -- 表名
    ) as table_pop
on 1=1;


-- --------------------------------------------------------------
-- 创建 样本 女 app行为 分布表 bsn_behavior_dist_w
-- --------------------------------------------------------------
drop table if exists bsn_behavior_dist_w;      -- 表名
create table if not exists bsn_behavior_dist_w -- 表名
    (
    behavior string,                       -- 特征名
    uv int,
    bsn_per float                        -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into bsn_behavior_dist_w           -- 表名
select
    table_label.label,
    table_label.label_uv,
    table_label.label_uv / table_pop.pop_uv as rate 
from 
    (
    select behavior as label, count(mid) as label_uv from bsn_behavior_w group by behavior   -- 表名，字段名*2
    ) as table_label  
left join
    (
    select count(mid) as pop_uv from bsn_behavior_w      -- 表名
    ) as table_pop
on 1=1;




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

