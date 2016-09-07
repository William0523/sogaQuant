#! -*- coding:utf-8 -*-
from quant.core.Selecter import *
from decimal import Decimal


class RealVolumeRateSelecter(Selecter):
    '''
    实时成交与流通盘比率,观察当日热点
    '''
    def __init__(self, setting):
        Selecter.__init__(self, setting)
        self.setting = setting

    def add_new_field(self, x):
        #基础数据取trade表,st部分数据缺失
        x.bs_rate = 0.00001
        if x.run_market > 0:
            x.bs_rate = (x.amount*1.0)/x.run_market
        return x

    def run(self):
        #成交/流通盘
        self.todayDF.insert(1, 'bs_rate', 0)
        self.todayDF = self.todayDF[self.todayDF.run_market > 1000000000]
        self.todayDF = self.todayDF[self.todayDF.chg > Decimal(-5)]

        t = time.time()-180*86400
        listing_date = self.tools.d_date('%Y%m%d', t)
        listing_date = 20151201
        #a = self.todayDF[self.todayDF.s_code == 'sz300104']
        self.todayDF = self.todayDF.apply(self.add_new_field, axis=1)
        _data = self.todayDF[self.todayDF.bs_rate > 0.095].sort_values(by=('bs_rate'), ascending=False)
        #print _data
        #sys.exit()
        #查看昨天列表与当天
        sql_data = "select * FROM s_stock_list WHERE dateline=%s " % self.lastDay
        tmpdf = pandas.read_sql(sql_data, self.mysql.db)

        res = []
        for i in range(0, len(_data)):
            curr = tmpdf[tmpdf.s_code == _data.iloc[i].s_code]
            #过滤近一个月上线的新股
            #print _data.iloc[i]
            #print curr.listing_date
            #continue
            #sys.exit()

            if int(curr.listing_date) > int(listing_date):
                continue

            run_market = int(curr.run_market)/10000
            #流通小于20亿
            if run_market < 200000:
                continue
            curr = curr.iloc[0].chg
            if curr < -4:
                continue
                #sys.exit()
            print "%s==%s==%s===%s==%s=" % (_data.iloc[i].s_code, _data.iloc[i]['name'],  _data.iloc[i].chg, curr, run_market)
            #sys.exit()
            res.append(_data.iloc[i].s_code)

        return res
        #sys.exit()
