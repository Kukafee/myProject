-- /Users/edz/pang/code/sublime/task_1.sql
------------------------------------------------------------
-- 0.创建数据库pangxk_x
------------------------------------------------------------
drop database if exists pangxk_1 cascade;
create database if not exists pangxk_1;

------------------------------------------------------------
-- 1.
------------------------------------------------------------


-- 
use pangxk_1;
select current_database();

drop table if exists mid_ttlabel;
create table if not exists mid_ttlabel(
mid string,
sex int,
ttsex string,
age int, 
ttage string,
area string
)
partitioned by (
op string comment 'operator',
m int comment 'month')
row format delimited
fields terminated by ','
;

-- 创建数据表<mid~ttsex>
drop table if exists mid_ttsex;
create table if not exists mid_ttsex(
	mid string,
	ttsex string
	-- ttage string,
	-- ttarea string
	)
partitioned by (
op string comment 'operator',
m int comment 'month')
row format delimited
fields terminated by ','
-- collection items t erminated by  ' '
-- map keys terminated by ':'
;

-- 创建性别映射表
drop table if exists sex_ttsex;
create table if not exists sex_ttsex(
sex int,
ttsex string)
partitioned by (
op string comment 'operator',
m int comment 'month')
row format delimited fields terminated by ',';
-- 插入数据
insert into table sex_ttsex 
partition(op='liantong', m=201909) 
values
(0, '男'),
(1, '女'),
(-1, '未知');


-- 创建数据表<mid~ttage>
drop table if exists mid_ttage;
create table if not exists mid_ttage(
	mid string,
	ttage string
	)
partitioned by (
op string comment 'operator',
m int comment 'month')
row format delimited
fields terminated by ','
-- collection items t erminated by  ' '
-- map keys terminated by ':'
;



-- 创建数据表<mid~ttarea>
drop table if exists mid_ttarea;
-- create table if not exists mid_ttarea(
-- 	mid string,
-- 	ttarea string
-- 	)
-- partitioned by (
-- op string comment 'operator',
-- m int comment 'month')
-- row format delimited
-- fields terminated by ','
-- collection items t erminated by  ' '
-- map keys terminated by ':'
-- ;

insert into table mid_ttlabel (mid, ttsex) partition(op='liantong', m=201909)
select databank.freq_info.mid, databank.freq_info.sex, mid_ttlabel.ttsex
from databank.freq_info where partition(op='liantong' and m=201909
join pangxk_1.sex_ttsex
on freq_info.sex = sex_ttsex.sex;

select mid, sex, ttsex from (select mid, sex from databank.freq_info) as t1 join (select sex, ttsex from sex_ttsex) as t2 on t1.sex = t2.sex; 


insert into table mid_ttlabel (mid, ttsex)
partition(op='liantong', m=201909)
select mid, ttsex
from (select mid, sex from databank.freq_info) as t1
join (select sex, ttsex from sex_ttsex) as t2
on t1.sex = t2.sex;







