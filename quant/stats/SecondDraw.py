# -*- coding: utf-8 -*-
import sys
from settings import *
import pandas
from quant.core.Stats import *
'''
处理 NA 的方法有四种：dropna , fillna , isnull , notnull
pandas.isnull(obj)和pandas.notnull(obj) 返回true or false，一般可用于做布尔型索引。
pandas.dropna()

对于Series，DataFrame和Panel都有dropna()的方法，参数不尽相同。我们拿DataFrame看一下。
去掉出现na值的行，即obj.dropna()即可。
去掉所有值为na的列，obj.dropna(axis = 1, how=’all’)

- pandas.fillna()

对于Series，DataFrame和Panel都有fillna()的方法，参数不尽相同。同样拿DataFrame看一下。

'''


class SecondDrawStats(StatsEngine):

    def __init__(self, args):
        StatsEngine.__init__(self)
        self.args = args

    def run(self):
        sql_data = "select * FROM s_stock_runtime WHERE dateline =20160607 and s_code='sh600774' "
        tmpdf = pandas.read_sql(sql_data, self.mysql.db)
        pandas.set_option('display.width', 400)
        res = {}
        for i in range(len(tmpdf)):
            item = tmpdf.iloc[i]
            #inf = ''
            if item.s_code not in res.keys():
                res[item.s_code] = {'B': 0, 'S': 0}

            if item.B_1_volume > 100000:
                res[item.s_code]['B'] += 1
            if item.B_2_volume > 100000:
                res[item.s_code]['B'] += 1
            if item.B_3_volume > 100000:
                res[item.s_code]['B'] += 1
            if item.B_4_volume > 100000:
                res[item.s_code]['B'] += 1
            if item.B_5_volume > 100000:
                res[item.s_code]['B'] += 1

            if item.S_1_volume > 100000:

                res[item.s_code]['S'] += 1
            if item.S_2_volume > 100000:
                #print item
                res[item.s_code]['S'] += 1
            if item.S_3_volume > 100000:
                res[item.s_code]['S'] += 1
            if item.S_4_volume > 100000:
                res[item.s_code]['S'] += 1
            if item.S_5_volume > 100000:
                res[item.s_code]['S'] += 1

        print res



    def min_data(self):
        sql_data = "select * FROM s_stock_runtime WHERE dateline =20160607 and s_code='sz000048' "
        tmpdf = pandas.read_sql(sql_data, self.mysql.db)
        pandas.set_option('display.width', 400)
        # 设定转换的周期period_type，转换为周是'W'，月'M'，季度线'Q'，五分钟是'5min'，12天是'12D'
        period_type = 'W'
        #数据清洗

        # 将【date】设定为index
        tmpdf.set_index('date_str', inplace=True)
        period_stock_data = tmpdf.resample('1Min', how='last')
        #period_stock_data =
        #print len(period_stock_data)
        #print period_stock_data['B_1_price'].sum()
        period_stock_data['MA_1'] = pandas.rolling_mean(period_stock_data['B_1_price'], 1)
        #period_stock_data = tmpdf.resample('5Min', how='last')
        print period_stock_data
        sys.exit()
        df = pandas.DataFrame(columns=('k', 'v'))
        data = {}
        j = 0
        for i in range(len(tmpdf)):
            #print tmpdf.iloc[i]
            _min = tmpdf.iloc[i].min_sec
            #print _min
            if _min > 150000 and '150000' in data.keys():
                continue
            _min = str(_min)

            _min = _min[0:-2]
            #print _min
           # sys.exit()
            #[0:-2]

            _min_str = "%s00" % _min
            #data[_min_str] =

            if _min_str not in data.keys():
                #data = {'k': _min_str, 'v': tmpdf.iloc[i].B_1_price}
                j += 1

            data[_min_str] = {'v': tmpdf.iloc[i].B_1_price}

            df.loc[j] = {'k': _min_str, 'v': tmpdf.iloc[i].B_1_price}
            #j += 1
            #data.append(_v)
            #sys.exit()
        print df
        #print tmpdf
