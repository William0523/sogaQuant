#!/bin/bash
TODAY=`date +%Y%m%d`
echo $TODAY

python daily.py get_index_data $TODAY
echo "Stock Index done.\n"

#php /htdocs/quant/soga/mv/index.php Base get_fq_factor
#echo "FuQuan Done\n"

#php /htdocs/quant/soga/mv/index.php Base daily_stock_list
#echo "Stock daily done.\n"

python daily.py get_lhb_data $TODAY
echo "LHB stock list done.\n"

python daily.py summary_report $TODAY
echo "Stock Day_Report done.\n"

#python daily.py count_lhb_data $TODAY

#python multi.py get_multi_close_data $TODAY
#echo "Stock daily closing Bid.\n"

#python daily.py summary_average $TODAY
#echo "Average & MA_count Done."

#python daily.py get_macount $TODAY
#echo "MA_count Done."



#python /htdocs/quant/soga/main.py daily 20160620

#qiniu /root/anaconda2/bin/python /home/wwwroot/quant/site.py get_tt_user_video
#runtime
#python /htdocs/quant/soga/realtime.py while_change
#python realtime.py get_min_data


#python /htdocs/quant/soga/realtime.py pankou_save 1
#python /htdocs/quant/soga/realtime.py pankou_save 2
#python /htdocs/quant/soga/realtime.py pankou_save 3
#python /htdocs/quant/soga/realtime.py pankou_save 4

#python /htdocs/quant/soga/realtime.py pankou_open
#python /htdocs/quant/soga/realtime.py pankou_realtime
#python /htdocs/quant/soga/realtime.py pankou_replay 20160629
#python /htdocs/quant/soga/realtime.py follow_yyb 20160812

#python /htdocs/quant/soga/realtime.py pankou_five_order 000610


#python /htdocs/quant/soga/realtime.py xq_ltgd 1
#python /htdocs/quant/soga/realtime.py xq_shareholdernum 1

#python /htdocs/quant/soga/realtime.py gudong 300280
#python /htdocs/quant/soga/realtime.py gudong_name 李白
#python /htdocs/quant/soga/realtime.py get_new_gudong 1
#python /htdocs/quant/soga/realtime.py get_jj_hold 1
#股东变化
#python /htdocs/quant/soga/realtime.py get_gudong_change 1


#python /htdocs/quant/soga/realtime.py save_big_order 20160930
#python /htdocs/quant/soga/realtime.py stats_big_order 20160930 A|B|C


#python /htdocs/quant/soga/realtime.py save_multi_bs_order 20161202
#python /htdocs/quant/soga/realtime.py count_bs_order 20161202
#python /htdocs/quant/soga/realtime.py stats_bs_order 20161202

#股东组合
#python /htdocs/quant/soga/realtime.py get_gudong_zuhe


#分笔买卖
#python /htdocs/quant/soga/realtime.py save_bs_order 1
#python /htdocs/quant/soga/realtime.py count_bs_order 1

#python /htdocs/quant/soga/site.py baidu_words_a
#python /htdocs/quant/soga/site.py baidu_words_b
#python /htdocs/quant/soga/site.py baidu_words_second 46195

#python /htdocs/quant/soga/site.py save_video 1 v1

