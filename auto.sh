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

#python /htdocs/quant/soga/realtime.py xue_qiu_ltgd 1
#python /htdocs/quant/soga/realtime.py gudong sz300280
#python /htdocs/quant/soga/realtime.py gudong_name 李白
#python /htdocs/quant/soga/realtime.py get_new_gudong 1

#python /htdocs/quant/soga/site.py baidu_words_a
#python /htdocs/quant/soga/site.py baidu_words_b
#python /htdocs/quant/soga/site.py baidu_words_second 46195

#python /htdocs/quant/soga/site.py save_video 1 v1
#python /htdocs/quant/soga/realtime.py xue_qiu_ltgd 1

