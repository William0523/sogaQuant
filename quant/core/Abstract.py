#! -*- coding:utf-8 -*-
import os
import json
import signal
import time
import logging
import sys
import hashlib
from quant.core.DB import sMysql
from quant.tools.Util import sTools


class Abstract(object):
    benchmart_start = 0
    benchmart_end = 0

    def __init__(self):
        self.tools = sTools()
        self.benchmart_start = time.clock()
        self.__read_config()
        mysql_db = self.config['mysql']['stock']
        self.mysql = sMysql(mysql_db['host'], mysql_db['user'], mysql_db['password'], mysql_db['dbname'])

    interrupted = False

    def __del__(self):
        self.benchmart_end = time.clock()
        time_str = "Runtime: %f s" % (self.benchmart_end - self.benchmart_start)
        date = time.strftime('%Y-%m-%d %H:%M:%S')
        host = self
        pid = os.getpid()
        header = '{0} {1}[{2}]:\n{3}\n'
        print header.format(date, host, pid, time_str)

    def is_opening(self):
        #判断当前时间是否为开盘时间
        res = True
        block_time = int(self.tools.d_date('%H%M%S'))
        if (block_time > 113000 and block_time < 130000) or block_time > 153000 or block_time < 93000:
            res = False
        return res

    def signal_handler(self, signum, frame):
        if signum == signal.SIGTERM or signum == signal.SIGINT:
            self.interrupted = True

    def read_yyb_config(self):
        self.config_path = os.path.dirname(__file__) + '/../../yyb.json'
        return self.file2dict(self.config_path)

    def __read_config(self):
        """读取 config"""
        self.config_path = os.path.dirname(__file__) + '/../../config.json'
        self.config = self.file2dict(self.config_path)
        #self.global_config = self.file2dict(self.global_config_path)
        #self.config.update(self.global_config)

    def file2dict(self, path):
        with open(path) as f:
            return json.load(f)

    def run_php(self, path):
        cmd = 'php /htdocs/quant/soga/mv/index.php %s' % path
        os.system(cmd)

    def get_md5(self, s):
        s = s.encode('utf8') if isinstance(s, unicode) else s
        m = hashlib.md5()
        m.update(s)
        return m.hexdigest()

    def get_hash_key(self, long_url):
        #生成短域名
        code_map = ('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z')
        hkeys = []
        hex = self.get_md5(long_url)
        for i in xrange(0, 4):
            n = int(hex[i*8:(i+1)*8], 16)
            v = []
            e = 0
            for j in xrange(0, 5):
                x = 0x0000003D & n
                e |= ((0x00000002 & n) >> 1) << j
                v.insert(0, code_map[x])
                n = n >> 6
            e |= n << 5
            v.insert(0, code_map[e & 0x0000003D])
            hkeys.append(''.join(v))
        return hkeys
