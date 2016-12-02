# -*- coding: utf-8 -*-
import sys
import time
from quant.spider.MinData import *
#from quant.spider.Handicap import *
#from quant.core.DB import sMysql

from quant.stats.RealTimeChange import *
from quant.stats.SecondDraw import *
from quant.stats.Pankou import *
from quant.stats.Department import *

from quant.stats.F10 import *

from quant.spider.Stock import *
from quant.spider.LhbData import *


def __read_config():
    """读取 config"""
    config_path = 'config.json'
    config = file2dict(config_path)
    return config


def file2dict(path):
    with open(path) as f:
        return json.load(f)


def get_min_data(abc):
    #20分钟一次获取各股的长跌幅
    s = MinDataSpider()
    s.run()


def while_change():
    #5分钟一次获取各股的涨跌幅
    RealTimeChange(sys.argv).run()


def demo():
    SecondDrawStats(sys.argv).run()


def follow_yyb():
    Department().daily()


def pankou_replay():
    #盘后 对挂单超过2KW的单子进行监控
    Pankou().replay()


def pankou_realtime():
    #实时盘口显示,超1KW
    Pankou().realtime()


def pankou_open():
    #每日open last_close 写入
    Pankou().daily_open()


def pankou_save():
    '''
    获取实时5档买卖盘口
    python realtime.py get_five_sb 页数
    '''
    mcache = memcache.Client(['127.0.0.1:11211'])
    while 1:
        Pankou().save(mcache)


def save_big_order():
    Pankou().save_stock_big_order()


def stats_big_order():
    #10jqk
    Pankou().big_order_list()


def stats_bs_order():
    Pankou().stock_bs_big_order()


def save_multi_bs_order():
    #保存分笔数据
    StockSpider().save_multi_data()


def count_bs_order():
    #统计分笔数据
    Pankou().stats_bs_order()


def xq_ltgd():
    #雪球流通股东
    StockSpider().get_ltgd()


def xq_shareholdernum():
    StockSpider().get_xueqiu_shareholdernum()


def show_15():
    #显示定期更新的股东人数
    StockSpider().show_shareholdernum()


def gudong():
    #历史股东,龙虎榜,大单,5档挂单
    F10().history_data()


def gudong_name():
    F10().search_gudong_name()


def get_new_gudong():
    F10().get_new_gudong()


def get_jj_gudong():
    #基金持股家数
    F10().get_jj_stock()


def get_jj_hold():
    F10().search_jj_list()


def get_gudong_change():
    F10().gudong_change()


def get_gudong_zuhe():
    #股东组合持仓
    F10().gudong_zuhe_stock()


def get_gudong_majia():
    #股东共同操作特定股票
    F10().find_zh_majia()


def pankou_five_order():
    #查看某只股票的5档盘口异常历史
    Pankou().five_order_stock(sys.argv[2])


def pankou_daily_five_order():
    #每日异常
    Pankou().five_daily_list(sys.argv[2])


def history_lhb():
    LhbDataSpider().show_daily_lhb()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()

    parser.add_argument('--nimbus-host', default='127.0.0.1')
    parser.add_argument('--nimbus-port', type=int, default=6627)
    #args = parser.parse_args()
    #Job(args).run()
    #sys.exit()
    start = time.time()
    function = eval(sys.argv[1])
    function()
    end = time.time()
    print end-start
