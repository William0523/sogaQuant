# -*- coding: utf-8 -*-
import os
import sys
import time

from quant.spider.LhbData import *
from quant.spider.Index import *
from quant.spider.TouTiao import *
from quant.spider.TouTiaoDetail import *
from quant.spider.Stock import *
from quant.spider.Fund import *

from quant.stats.ElSearch import *
from quant.stats.Average import *
from quant.stats.MaCount import *
from quant.stats.Summary import *

from quant.stats.TdxAnalyse import *
#from seleninum import webdriver


def get_tdx(indate):
    ffile = '/Users/solomon/Downloads/008/%s.xls' % date
    tfile = '/Users/solomon/Downloads/008/%s.csv' % date
    a = TdxAnalyse()
    a.run(ffile, tfile)


def get_factor(abc):
    os.system('php /htdocs/soga/trader/index.php Base get_fq_factor')


def get_es(abc):
    a = ElSearch(sys.argv)
    a.run()


def summary_average(abc):
    #均线统计
    Average(sys.argv).run()
    MaCount(sys.argv).run()


def summary_report(abc):
    #每日上涨下跌、开板等数据统计
    Summary(sys.argv).run()


def get_lhb_data(abc):
    LhbDataSpider().run()


def get_index_data(abc):
    IndexSpider().run()


def get_fund_data(abc):
    FundSpider().run()


def get_base_data(abc):
    StockSpider().run()


class Job:
    interrupted = False

    def __init__(self, args):
        self.args = args

    def signal_handler(self, signum, frame):
        if signum == signal.SIGTERM or signum == signal.SIGINT:
            self.interrupted = True

    def run(self):
        print self.args

if __name__ == '__main__':
    import argparse
    import signal
    parser = argparse.ArgumentParser()

    parser.add_argument('--nimbus-host', default='127.0.0.1')
    parser.add_argument('--nimbus-port', type=int, default=6627)
    #args = parser.parse_args()
    #Job(args).run()
    #sys.exit()
    start = time.time()
    function = eval(sys.argv[1])
    function(sys.argv[2])
    end = time.time()
    print end-start
