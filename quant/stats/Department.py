# -*- coding: utf-8 -*-
import sys
default_encoding = 'utf-8'
if sys.getdefaultencoding() != default_encoding:
    reload(sys)
    sys.setdefaultencoding(default_encoding)
from quant.core.Spider import *


class Department(SpiderEngine):
    '''
    实时盘口统计
    '''
    def __init__(self):
        SpiderEngine.__init__(self)

    def daily(self):
        f_yyb = self.read_yyb_config()
        self.today = sys.argv[2]
        yyb_ids = []
        yyb_items = {}
        yyb_rel = {}
        for i in range(0, len(f_yyb)):
            dep = f_yyb[i]
            for j in range(0, len(dep['items'])):
                yyb_ids.append(int(dep['items'][j]['yybid']))
                yyb_items[int(dep['items'][j]['yybid'])] = "%s==%s" % (f_yyb[i]['area'], dep['items'][j]['name'])
                yyb_rel[int(dep['items'][j]['yybid'])] = f_yyb[i]['area']

        stock = self.mysql.getRecord("select * from s_stock_list where 1")
        stock_list = {}
        for o in range(0, len(stock)):
            stock_list[stock[o]['s_code']] = stock[o]

        items = {}
        yybs = {}
        data = self.mysql.getRecord("select * from s_lhb_days_detail where dateline=%s" % self.today)
        for k in range(0, len(data)):
            ai = {
                'code': data[k]['s_code'],
                'T': data[k]['type'],
                'B': data[k]['B_volume']/10000,
                'B_p': data[k]['B_p'],
                'S': data[k]['S_volume']/10000,
                'S_P': data[k]['S_p'],
                'sort': data[k]['s_sort'],
                'yyb_id': data[k]['yyb_id'],
            }
            #关注的营业部
            if data[k]['yyb_id'] in yyb_ids:
                yyb_area = yyb_rel[data[k]['yyb_id']]
                if data[k]['s_code'] not in items.keys():
                    items[data[k]['s_code']] = {}
                if yyb_area not in yybs.keys():
                    yybs[yyb_area] = []

                items[data[k]['s_code']][data[k]['yyb_id']] = ai
                yybs[yyb_area].append(ai)
        print '\033[1;31;43m'
        print "=============================代码====================================="
        print '\033[0m'
        for k, v in items.items():
            print '\033[1;31;40m'
            print "%s%s==%s" % ('*' * 20, k, stock_list[k]['name'])
            print '\033[0m'
            for kk, vv in v.items():
                #print len(yyb_items[kk])
                vm = vv['B'] - vv['S']
                vb = 'B'
                vm_color = 40
                if vm < 0:
                    vb = 'S'
                    vm_color = 36
                vmoney = "\033[41;%sm%s%sW \033[0m" % (vm_color, vb, vm)
                print "\t%s====%s" % (self.__format_str(yyb_items[kk], 20), vmoney)

        print '\033[1;31;43m'
        print "=============================按营业部====================================="
        print '\033[0m'

        for k, v in yybs.items():
            print '\033[1;31;40m'
            print "%s%s" % ('*' * 30, k)
            print '\033[0m'
            for i in range(0, len(v)):
                yyb_name = yyb_items[int(v[i]['yyb_id'])]
                vm = v[i]['B'] - v[i]['S']
                vb = 'B'
                show_BS = ''
                if vm < 0:
                    vb = 'S'
                    if v[i]['B'] > 0:
                        show_BS = "%s==%s" % (v[i]['B'], v[i]['S'])
                print "\t%s====%s====%s%sW==%s===%s===%s" % (v[i]['code'], stock_list[v[i]['code']]['name'], vb, vm, show_BS, v[i]['yyb_id'], yyb_name)

    def __format_str(self, strA, maxL):
        l = len(strA)
        if l >= maxL:
            return strA
        return "%s%s" % (strA, '='*(maxL-l))
