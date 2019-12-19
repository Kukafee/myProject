--------------------------------------------------------
-- 创建数据库
--------------------------------------------------------
create database if not exists kylin_ana;

--------------------------------------------------------
--------------------------------------------------------
-- 资源表处理
--------------------------------------------------------
--------------------------------------------------------

--------------------------------------------------------
-- 1. 创建app_id对照表 done
--------------------------------------------------------
drop table if exists apps_id_table;
create table if not exists apps_id_table (
    app_id string, 
    app string )
row format delimited
fields terminated by '\t'
collection items terminated by '-'
map keys terminated by ':';
load data local inpath '/kylin_ana/tables_to_load/app_id.table' overwrite into table apps_id_table;

--------------------------------------------------------
-- 2. 创建app_info对照表 done
--------------------------------------------------------
drop table if exists apps_info_table;
create table if not exists apps_info_table (
    app string, 
    media string, 
    stage string, 
    class string, 
    subclass string, 
    hobby string )
row format delimited
fields terminated by '\t'
collection items terminated by '-'
map keys terminated by ':';
load data local inpath '/kylin_ana/tables_to_load/app_info.table' overwrite into table apps_info_table;

--------------------------------------------------------
-- 3. 创建省份编码对照表 done
--------------------------------------------------------
drop table if exists province_code_table;
create table if not exists province_code_table (
    province string, 
    cn_code string,
    raw_province string )
row format delimited
fields terminated by '\t'
collection items terminated by '-'
map keys terminated by ':';
load data local inpath '/kylin_ana/tables_to_load/province_code.table' overwrite into table province_code_table;


--------------------------------------------------------
--------------------------------------------------------
-- 用户总体信息统计
--------------------------------------------------------
--------------------------------------------------------
--------------------------------------------------------
-- 4. 创建用户appid对照表 done
--------------------------------------------------------
drop table if exists phone_appid_table;
create table if not exists phone_appid_table(
    phone string,
    app_id string,
    daycount int)
partitioned by (op string)
row format delimited
fields terminated by '\t'
collection items terminated by '-'
map keys terminated by ':';

-- 填充用户行为表格
insert overwrite table phone_appid_table partition(op='op1')
    select phone,key as app_id,value['daycount'] as daycount 
    from databank.user_info_freq lateral view explode(app_freq) temp
    where op='op1' and m=201901;
insert overwrite table phone_appid_table partition(op='op2')
    select phone,key as app_id,value['daycount'] as daycount 
    from databank.user_info_freq lateral view explode(app_freq) temp
    where op='op2' and m=201901;

--------------------------------------------------------
-- 5. 总体结果 ： 用户基础信息表 全体用户
--------------------------------------------------------
drop table if exists user_info_table;
create table if not exists user_info_table(
    phone string,
    sex int,
    age int,
    income int,
    l_province string,
    l_city string,
    cn_code string)
partitioned by (op string)
row format delimited
fields terminated by '\t'
collection items terminated by '-'
map keys terminated by ':';

insert overwrite table user_info_table partition(op='op1')
    select distinct phone, sex, age, income, province, l_city, cn_code
    from province_code_table 
    inner join (select phone, sex, age, income, l_province, l_city from databank.user_info where op='op1') as table_a
    on province_code_table.raw_province=table_a.l_province;

insert overwrite table user_info_table partition(op='op2')
    select distinct phone, sex, age, income, province, l_city, cn_code
    from province_code_table 
    inner join (select phone, sex, age, income, l_province, l_city from databank.user_info where op='op2') as table_a
    on province_code_table.raw_province=table_a.l_province;


--------------------------------------------------------
-- 6. 生成用户app对照表
--------------------------------------------------------
drop table if exists phone_app_table;
create table if not exists phone_app_table(
    phone string,
    app string,
    daycount int)
partitioned by (op string)
row format delimited
fields terminated by '\t'
collection items terminated by '-'
map keys terminated by ':';

-- 填充用户行为表格
insert overwrite table phone_app_table partition(op='op1')
    select phone,app,max(daycount) as daycount
    from apps_id_table 
    inner join (select * from phone_appid_table where op='op1') as table_a 
    on apps_id_table.app_id=table_a.app_id
    group by phone,app;
    
insert overwrite table phone_app_table partition(op='op2')
    select phone,app,max(daycount) as daycount
    from apps_id_table 
    inner join (select * from phone_appid_table where op='op2') as table_a 
    on apps_id_table.app_id=table_a.app_id
    group by phone,app;

drop table if exists phone_appid_table;

--------------------------------------------------------
-- 7.总体结果 ： 生成用户行为主视图 全体用户
--------------------------------------------------------
drop view if esists phone_behavior_view;
create view if not exists phone_behavior_view
as select 
    phone,
    phone_app_table.app,
    daycount,
    media,
    stage,
    class,
    subclass,
    hobby
from apps_info_table
inner join phone_app_table
on apps_info_table.app=phone_app_table.app;


--------------------------------------------------------
--------------------------------------------------------
-- 业务样本信息统计
--------------------------------------------------------
--------------------------------------------------------
--------------------------------------------------------
-- 8. temp 生成业务表 
--------------------------------------------------------
use  kylin_ana;
drop table if exists edu_groups_table;
create table if not exists edu_groups_table(
    bsngroup string,
    subgroup string)
row format delimited
fields terminated by '\t'
collection items terminated by '-'
map keys terminated by ':';
load data local inpath '/kylin_ana/tables_to_load/edu_groups.table' overwrite into table edu_groups_table;

-- 第一种方式
drop table if exists bsn_education_table;
create table if not exists bsn_education_table
    as 
    select phone,bsngroup,subgroup from phone_app_table
    inner join edu_groups_table on edu_groups_table.subgroup=phone_app_table.app;


--------------------------------------------------------
-- 9.sample: 生成业务主视图
--------------------------------------------------------

drop view if esists bsn_education_static_view;
create view if not exists bsn_education_static_view
as select 
    bsn_education_table.phone,
    bsngroup,
    subgroup,
    sex,
    age,
    income,
    l_province,
    l_city,
    cn_code
from bsn_education_table
inner join user_info_table
on bsn_education_table.phone=user_info_table.phone;


drop view if exists bsn_education_behavior_view;
create view if not exists bsn_education_behavior_view
as select 
    bsn_education_table.phone,
    bsngroup,
    subgroup,
    phone_app_table.app,
    daycount,
    media,
    stage,
    class,
    subclass,
    hobby
from bsn_education_table
inner join phone_app_table
on bsn_education_table.phone=phone_app_table.phone
inner join apps_info_table
on phone_app_table.app=apps_info_table.app;