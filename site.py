# -*- coding: utf-8 -*-
import sys
import time
#video
from quant.spider.TouTiao import *
from quant.spider.TouTiaoDetail import *
#weixin
from quant.spider.WeiXin import *
#baidu
from quant.spider.Baidu import *

from quant.spider.Demo import *

from quant.spider.Tao import *


def Usage():
    print 'site.py usage:'
    print '\trun_demo:\tTest.'
    print '\tget_tt_video:\t今日头条'
    print '\tpost_tt_video:\t今日头条发布'
    print '\tget_yy_video:\tYY室间列表'
    print '\tget_vlook_video:\t微录'
    print '\tget_wx_data:\t微信'
    print '\tbaidu_words_a:\t3500一级词'
    print '\tbaidu_words_b:\t15W一级词'
    print '\tbaidu_words_second:\t二级词'
    print '\n'


def run_demo():
    DemoSpider().run()


def get_tao_video():
    TaoSpider().run()


def get_tt_video():
    TouTiaoSpider().run_list()


def get_tt_user_video():
    TouTiaoSpider().run_user_list()


def post_tt_video():
    TouTiaoSpider().run_post()


def get_yy_video():
    TouTiaoSpider().run_yy()


def get_vlook_video():
    TouTiaoSpider().run_vlook()


def save_video():
    TouTiaoSpider().save_video()


def get_wx_data():
    #230232
    WeiXinSpider().run()


def baidu_words_a():
    #3577
    BaiduSpider().while_base_words_A()


def baidu_words_b():
    #15W
    BaiduSpider().while_base_words()


def baidu_words_second():
    #二级词
    BaiduSpider().get_word_pages()


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
    '''
    import argparse
    import signal
    parser = argparse.ArgumentParser()

    parser.add_argument('--nimbus-host', default='127.0.0.1')
    parser.add_argument('--nimbus-port', type=int, default=6627)
    '''
    print sys.argv
    if len(sys.argv) < 2:
        Usage()
        sys.exit()

    start = time.time()
    function = eval(sys.argv[1])
    function()
    end = time.time()
    print end-start
