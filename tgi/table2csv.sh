#!/bin/bash
# chmod +x table2csv.sh
path="/home/wangl/pangxk/tgi/"
sqlorder="set hive.cli.print.header=true; select * from pangxk."
csv=".csv"
cd $path
feature_array=("tgi_sex" "tgi_age" "tgi_province" "tgi_city" "tgi_l_province" "tgi_l_city")


for ((i=0;i<${#feature_array[@]};i++))
do
	touch ${feature_array[$i]}
	hive -e $sqlorder${feature_array[$i]} > $path${feature_array[$i]}$csv
done

# touch tgi_sex.csv

echo Done!



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
