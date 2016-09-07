# -*- coding: utf-8 -*-
import sys
import logging
import datetime
import time
import hashlib
from quant.core.Spider import *


class FundSpider(SpiderEngine):
    '''
    基金数据
    '''
    def __init__(self):
        SpiderEngine.__init__(self)

    #def get_info(self, yyb_id):
        #self.get_daily_detail(yyb_id, sys.argv[2])

    def run(self):
        print sys.argv
        self.tools.setup_logging(sys.argv[1], True, True)
        logging.debug('Start Fund Company Data=====')
        #self.get_fund_company()
        #self.get_fund_gscc(1)
        #self.get_fund_jjcc(1)
        #self.get_all_fund()
        #return
        x = self.mysql.getRecord("SELECT * FROM  `s_fund_jj` WHERE  id > 3474")
        for k in range(0, len(x)):
            self.get_fund_jjcc(x[k])

    def get_fund_company(self):
        #基金公司
        '''
        url = 'http://fund.eastmoney.com/company/default.html'
        _data = self.sGet(url)
        _data = self.sMatch('<div class="stcompany">', '<\/div>', _data, 0)
        _list = self.sMatch('<a href="http:\/\/fund.eastmoney.com\/company\/(.*?)\.html">', '<\/a>', _data[0], 0)
        for i in range(0, len(_list)):
            if _list[i][1]:
                _has = self.mysql.fetch_one("select * from  s_fund_company where fund_id='%s'" % _list[i][0])
                _where = "fund_id='%s'" % _list[i][0]
                indata = {'fund_id': _list[i][0], 'fund_name': _list[i][1]}
                if _has is not None:
                    self.mysql.dbUpdate('s_fund_company', indata, _where)
                else:
                    self.mysql.dbInsert('s_fund_company', indata)
        '''
        #获取基金公司的各支基金
        x = self.mysql.getRecord("SELECT * FROM  `s_fund_company` WHERE  1")
        for k in range(0, len(x)):
            url = 'http://fund.eastmoney.com/company/f10/jjjz_%s.html' % x[k]['fund_id']
            print url
            _data = self.sGet(url)
            _data2 = self.sMatch('<div class="box">', "<\/table>", _data, 0)
            for a in range(0, len(_data2)):
                _tmp = self.sMatch("<label  class='left'>", "<\/label>", _data2[a], 0)
                #print _data2[a]
                if _tmp[0] == u'普通基金':
                    _td = self.sMatch("<a title='(.*?)' href='http:\/\/fund.eastmoney.com\/(.*?)\.html'>", "<\/a>", _data2[a], 0)
                    #print _td
                    for b in range(0, len(_td)):
                        indata = {
                            'company_code': x[k]['fund_id'],
                            'jj_code': _td[b][1],
                            'jj_name': _td[b][0]
                        }
                        _has = self.mysql.fetch_one("select * from  s_fund_jj where jj_code='%s'" % indata['jj_code'])
                        _where = "jj_code='%s'" % indata['jj_code']
                        if _has is not None:
                            self.mysql.dbUpdate('s_fund_jj', indata, _where)
                        else:
                            self.mysql.dbInsert('s_fund_jj', indata)
                    #sys.exit()
                    break

            #sys.exit()

        #print _list

    def get_all_fund(self):
        url = 'http://fund.eastmoney.com/Data/Fund_JJJZ_Data.aspx?t=1&lx=1&letter=&gsid=&text=&sort=zdf,desc&page=1,99999&feature=|&dt=1460040511424&atfc=&onlySale=0'
        _data = self.sGet(url, 'utf8')
        _data = self.sMatch('\[\[', '\]\]', _data, 0)
        _res = _data[0].split('],[')
        for i in range(0, len(_res)):
            _resB = _res[i].split('","')
            indata = {
                'company_code': 0,
                'jj_code': _resB[0].replace('"', ''),
                'jj_name': _resB[1]
            }
            _has = self.mysql.fetch_one("select * from  s_fund_jj where jj_code='%s'" % indata['jj_code'])
            _where = "jj_code='%s'" % indata['jj_code']
            if _has is not None:
                self.mysql.dbUpdate('s_fund_jj', indata, _where)
            else:
                self.mysql.dbInsert('s_fund_jj', indata)

    def _format_char(self, strs):
        strs = strs.replace('>', '')
        strs = strs.replace('%', '')
        return strs

    def get_fund_jjcc(self, company):
        #单个基金持仓
        url = 'http://fund.eastmoney.com/f10/FundArchivesDatas.aspx?type=jjcc&code=%s&topline=500&year=&month=&rt=0.1365041979803301' % company['jj_code']
        #url = 'http://fund.eastmoney.com/f10/FundArchivesDatas.aspx?type=jjcc&code=000927&topline=500&year=&month=&rt=0.1365041979803301'
        logging.debug('Spider Get Url =====%s' % url)
        _data = self.sGet(url)
        _data = self.sMatch("<div class='box'>", "<div class='hide' id='gpdmList'>", _data, 0)
        for i in range(0, len(_data)):
            _list = _data[i]
            _jdate = self.sMatch("<font class='px12'>", "<\/font>", _list, 0)
            d = datetime.datetime.strptime(_jdate[0], "%Y-%m-%d")
            day = self.tools.d_date('%Y%m%d', time.mktime(d.timetuple()))
            lines = self.sMatch("<tr>", "<\/tr>", _list, 0)
            for j in range(1, len(lines)):
                _stock = self.sMatch("<a href='http:\/\/quote.eastmoney.com\/(.*?)\.html'>", "<\/a>", lines[j], 0)
                if len(_stock) == 0:
                    continue
                precent = self.sMatch("td class='tor'", "<\/td>", lines[j], 0)
                print precent
                print _stock
                #sys.exit()
                if len(precent) == 3:
                    f_percent = self._format_char(precent[0])
                    f_nums = self._format_char(precent[1])
                else:
                    f_percent = self._format_char(precent[2])
                    f_nums = self._format_char(precent[3])

                indata = {
                    'jj_code': company['jj_code'],
                    'company_code': company['company_code'],
                    'report_date': day,
                    's_code': _stock[0][0],
                    'f_percent': f_percent,
                    'f_nums': f_nums,
                }
                print indata
                #sys.exit()
                word = '%s%s%s' % (company['jj_code'], day, _stock[0][0])
                indata['hash'] = hashlib.md5(word).hexdigest()
                _has = self.mysql.fetch_one("select * from  s_fund_stock where hash='%s'" % indata['hash'])
                _where = " hash='%s'" % indata['hash']
                if _has is not None:
                    self.mysql.dbUpdate('s_fund_stock', indata, _where)
                else:
                    self.mysql.dbInsert('s_fund_stock', indata)
            #print _list
            logging.debug('Spider Done ID=====%s' % company['id'])
            #sys.exit()
    '''
    def get_fund_gscc(self, code):
        #基金公司持仓
        #code = 80365985
        #url = 'http://fund.eastmoney.com/company/f10/CompanyDataApi.aspx?type=gscc&code=%s&topline=100&year=&month=1&ftype=0&rt=0.9779448761689769' % code
        url = 'http://fund.eastmoney.com/company/f10/CompanyDataApi.aspx?type=gscc&code=80365985&topline=100&year=&month=1&ftype=0&rt=0.9779448761689769'
        logging.debug('Spider Get Url =====%s' % url)
        _data = self.sGet(url)
        _data = self.sMatch("<div class='box'>", "<div class='hide' id='gpdmList'>", _data, 0)
        #_list = self.sMatch("", "", _data[0], 0)
        for i in range(0, len(_data)):
            _list = _data[i]
            _jdate = self.sMatch("<font class='px12'>", "<\/font>", _list, 0)
            d = datetime.datetime.strptime(_jdate[0], "%Y-%m-%d")
            day = self.tools.d_date('%Y%m%d', time.mktime(d.timetuple()))
            #print _jdate
            #sys.exit()
            lines = self.sMatch("<tr>", "<\/tr>", _list, 0)
            #lines = lines[1]

           # print lines
            #sys.exit()
            for j in range(1, len(lines)):
                _stock = self.sMatch("<a href='http:\/\/quote.eastmoney.com\/(.*?)\.html'>", "<\/a>", lines[j], 0)
                precent = self.sMatch("td class='tor'", "<\/td>", lines[j], 0)
                #print _stock
                #print precent
                #sys.exit()
                indata = {
                    'f_code': code,
                    'report_date': day,
                    's_code': _stock[0][0],
                    'f_percent': self._format_char(precent[0]),
                    'f_nums': self._format_char(precent[1]),
                }
                word = '%s%s%s' % (code, day, _stock[0][0])
                indata['hash'] = hashlib.md5(word).hexdigest()
                _has = self.mysql.fetch_one("select * from  s_fund_stock where hash='%s'" % indata['hash'])
                _where = " hash='%s'" % indata['hash']
                if _has is not None:
                    self.mysql.dbUpdate('s_fund_stock', indata, _where)
                else:
                    self.mysql.dbInsert('s_fund_stock', indata)

                #print indata
                #sys.exit()

            #print _stock
            #sys.exit()
    '''
