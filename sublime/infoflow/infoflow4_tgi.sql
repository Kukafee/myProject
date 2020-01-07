
-- 使用数据库
use infoflow;

-- --------------------------------------------------------------
-- --------------------------------------------------------------
-- 创建 并计算特征tgi表
-- --------------------------------------------------------------
-- --------------------------------------------------------------

-- --------------------------------------------------------------
-- 创建 男性 年龄 tgi 数据表 tgi_age_m
-- --------------------------------------------------------------
drop table if exists tgi_age_m;        -- 表名
create table if not exists tgi_age_m   -- 表名
    (
    age string,                        -- 特征名
    tgi_value float
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into tgi_age_m    -- 表名

select 
    bsn_table.age,   -- 特征名
    -- bsn_table.bsn_per / pop_table.pop_per as rate
    cast(bsn_table.bsn_per / pop_table.pop_per as decimal(12,6)) as tgi_value
from 
    bsn_age_dist_m as bsn_table    -- 表bsn
inner join
    pop_age_dist_m as pop_table    -- 表pop
on
    bsn_table.age = pop_table.age     -- 特征名*2
order by tgi_value desc;





-- --------------------------------------------------------------
-- 创建 女性 年龄 tgi 数据表 tgi_age_w
-- --------------------------------------------------------------
drop table if exists tgi_age_w;        -- 表名
create table if not exists tgi_age_w   -- 表名
    (
    age string,                        -- 特征名
    tgi_value float
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into tgi_age_w    -- 表名

select 
    bsn_table.age,   -- 特征名
    cast(bsn_table.bsn_per / pop_table.pop_per as decimal(12,6)) as tgi_value
from 
    bsn_age_dist_w as bsn_table    -- 表bsn
inner join
    pop_age_dist_w as pop_table    -- 表pop
on
    bsn_table.age = pop_table.age     -- 特征名*2
order by tgi_value desc;






-- --------------------------------------------------------------
-- 创建 男性 文章分类 tgi 数据表 tgi_article_m
-- --------------------------------------------------------------
drop table if exists tgi_article_m;        -- 表名
create table if not exists tgi_article_m   -- 表名
    (
    article string,                        -- 特征名
    tgi_value float
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into tgi_article_m    -- 表名

select 
    bsn_table.article,   -- 特征名
    cast(bsn_table.bsn_per / pop_table.pop_per as decimal(12,6)) as tgi_value
from 
    bsn_article_dist_m as bsn_table    -- 表bsn
inner join
    pop_article_dist_m as pop_table    -- 表pop
on
    bsn_table.article = pop_table.article     -- 特征名*2
order by tgi_value desc;





-- --------------------------------------------------------------
-- 创建 女性 文章分类 tgi 数据表 tgi_article_w
-- --------------------------------------------------------------
drop table if exists tgi_article_w;        -- 表名
create table if not exists tgi_article_w   -- 表名
    (
    article string,                        -- 特征名
    tgi_value float
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into tgi_article_w    -- 表名

select 
    bsn_table.article,   -- 特征名
    cast(bsn_table.bsn_per / pop_table.pop_per as decimal(12,6)) as tgi_value
from 
    bsn_article_dist_w as bsn_table    -- 表bsn
inner join
    pop_article_dist_w as pop_table    -- 表pop
on
    bsn_table.article = pop_table.article     -- 特征名*2
order by tgi_value desc;



-- --------------------------------------------------------------
-- 创建 男性 app行为 tgi 数据表 tgi_behavior_m
-- --------------------------------------------------------------
drop table if exists tgi_behavior_m;        -- 表名
create table if not exists tgi_behavior_m   -- 表名
    (
    behavior string,                        -- 特征名
    tgi_value float
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into tgi_behavior_m    -- 表名

select 
    bsn_table.behavior,   -- 特征名
    cast(bsn_table.bsn_per / pop_table.pop_per as decimal(12,6)) as tgi_value
from 
    bsn_behavior_dist_m as bsn_table    -- 表bsn
inner join
    pop_behavior_dist_m as pop_table    -- 表pop
on
    bsn_table.behavior = pop_table.behavior     -- 特征名*2
order by tgi_value desc;




-- --------------------------------------------------------------
-- 创建 女性 app行为 tgi 数据表 tgi_behavior_w
-- --------------------------------------------------------------
drop table if exists tgi_behavior_w;        -- 表名
create table if not exists tgi_behavior_w   -- 表名
    (
    behavior string,                        -- 特征名
    tgi_value float
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into tgi_behavior_w    -- 表名

select 
    bsn_table.behavior,   -- 特征名
    cast(bsn_table.bsn_per / pop_table.pop_per as decimal(12,6)) as tgi_value
from 
    bsn_behavior_dist_w as bsn_table    -- 表bsn
inner join
    pop_behavior_dist_w as pop_table    -- 表pop
on
    bsn_table.behavior = pop_table.behavior     -- 特征名*2
order by tgi_value desc;


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


