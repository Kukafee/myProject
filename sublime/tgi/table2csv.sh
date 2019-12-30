#!/bin/bash
# chmod +x table2csv.sh

# 192.168.0.164 服务器的存储路径path
# path="/home/wangl/pangxk/tgi/"

# 获取当前时间
time1=$(date "+%m-%d_%H:%M")

# 192.168.0.194 服务器的存储路径path
path="/root/pangxk/tgi/"
cd $path
mkdir tgivalues$time1
cd tgivalues$time1
# sqlorder="set hive.cli.print.header=true;select * from pangxk."
# csv=".csv"
# dot=";"

# 共 10 个TGI表
# # feature_array=("tgi_sex" "tgi_age" "tgi_province" "tgi_city" "tgi_l_province" "tgi_l_city" "tgi_article" "tgi_behavior" "tgi_interest1" "tgi_interest1_2")
# feature_array=("tgi_sex" "tgi_age" "tgi_province" "tgi_city" "tgi_l_province" "tgi_l_city")

# for ((i=0;i<${#feature_array[@]};i++))
# do
# 	if [[ -a ${feature_array[$i]}$csv ]];then rm -f ${feature_array[$i]}$csv;fi
# 	touch ${feature_array[$i]}$csv
# 	hive -e $sqlorder${feature_array[$i]}$dot > $path${feature_array[$i]}$csv
# done
hive -e "set hive.cli.print.header=true;select * from pangxk.tgi_sex;" > ./tgi_sex.csv
hive -e "set hive.cli.print.header=true;select * from pangxk.tgi_age;" > ./tgi_age.csv
hive -e "set hive.cli.print.header=true;select * from pangxk.tgi_province;" > ./tgi_province.csv
hive -e "set hive.cli.print.header=true;select * from pangxk.tgi_city;" > ./tgi_city.csv
hive -e "set hive.cli.print.header=true;select * from pangxk.tgi_l_province;" > ./tgi_l_province.csv
hive -e "set hive.cli.print.header=true;select * from pangxk.tgi_l_city;" > ./tgi_l_city.csv
hive -e "set hive.cli.print.header=true;select * from pangxk.tgi_article;" > ./tgi_article.csv
hive -e "set hive.cli.print.header=true;select * from pangxk.tgi_behavior;" > ./tgi_behavior.csv
hive -e "set hive.cli.print.header=true;select * from pangxk.tgi_interest1;" > ./tgi_interest1.csv
hive -e "set hive.cli.print.header=true;select * from pangxk.tgi_interest1_2;" > ./tgi_interest1_2.csv



# touch tgi_sex.csv

echo Done!


# 
# tgi_sex.csv
# tgi_age.csv
# tgi_province.csv
# tgi_city.csv
# tgi_l_province.csv
# tgi_l_city.csv
# tgi_article.csv
# tgi_behavior.csv
# tgi_interest1.csv
# tgi_interest1_2.csv

# 本文件路径 /Users/edz/pang/code/sublime/tgi/table2csv.sh
# 以下是测试代码
# path="/home/wangl/pangxk/tgi/"
# sqlorder="set hive.cli.print.header=true; select * from pangxk_."
# csv=".csv"
# cd $path
# feature_array=("age_tgi")

# for ((i=0;i<${#feature_array[@]};i++))
# do
# 	touch ${feature_array[$i]}$csv
# 	hive -e "$sqlorder${feature_array[$i]}" > $path${feature_array[$i]}$csv
# done

# # touch tgi_sex.csv

# echo Done!

# if[ -f ${feature_array[$i]} ];then
# 	rm -f ${feature_array[$i]}
# fi

# if [[ -a age_tgi.csv ]];then echo "in";fi

# if [[ -a ${feature_array[$i]}$csv ]];then rm -f ${feature_array[$i]}$csv;fi
