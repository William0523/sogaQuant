# -*- coding: utf-8 -*-
import sys

from quant.core.DB import sMysql
from quant.tools.Util import sTools
from quant.core.Spider import SpiderEngine


def run_worker_template(params):
    id = 0
    while True:
        offset = id*params['limit']
        temp = mysql.getRecord("%s LIMIT %d, %d" % (params['sql'], offset, params['limit']))
        print "%s LIMIT %d, %d" % (params['sql'], offset, params['limit'])
        if len(temp) == 0:
            break
        else:
            data = []
            for row in temp:
                url = params['run_script'] % row.values()[0]
                if('ext' in params.keys()):
                    url += params['ext']
                data.append(url)
        id += 1
        spider.run_php_worker(data)
    '''
    def check_stock_status(params):
        arr = {}
        arr['sql'] = "SELECT s_code FROM s_stock_list WHERE 1 "
        arr['limit'] = 15
        arr['run_script'] = "php /htdocs/soga/trader/index.php Base get_stock_status_from_qq %s "
        arr['run_func'] = 'run_php_worker'
        return arr
     '''


#分钟
def get_stock_mintue(params):
    arr = {}
    arr['sql'] = "SELECT s_code FROM s_stock_list WHERE dateline=%s " % sTools.d_date('%Y%m%d')
    arr['limit'] = 15
    arr['run_script'] = "php /htdocs/soga/trader/index.php Base get_stock_mintue_from_sina %s "
    #arr['run_func'] = 'run_php_worker'
    return arr


def get_stock_mintue2(params):
    arr = {}
    arr['sql'] = "SELECT s_code FROM s_stock_list WHERE 1 "
    arr['limit'] = 15
    arr['run_script'] = "php /htdocs/soga/trader/index.php Base get_ff %s "
    #arr['run_func'] = 'run_php_worker'
    return arr


#每日收盘竞价
def get_multi_close_data(params):
    id = 0
    while True:
        offset = id*50
        temp = mysql.getRecord("SELECT s_code FROM s_stock_list WHERE dateline=20170413 LIMIT %d, %d" % (offset, 50))
        if len(temp) == 0:
            break
        else:
            data = []
            resx = []
            for row in temp:
                resx.append(row['s_code'])

            url = "php /htdocs/quant/soga/mv/index.php Base get_closing_bid_new %s %s" % (",".join(resx), sys.argv[2])
            data.append(url)
        id += 1
        print id
        spider.run_php_worker(data)


#历史数据
def get_stock_history_qq(params):
    arr = {}
    arr['sql'] = "SELECT s_code FROM s_stock_list WHERE id>1071 "
    arr['limit'] = 15
    arr['run_script'] = "php /htdocs/soga/trader/index.php Base build_average_data %s "
    #arr['run_script'] = "php /htdocs/soga/trader/index.php Base get_stock_daily_history %s "
    return arr


#东方财富网题材
def get_stock_dc_category(params):
    arr = {}
    arr['sql'] = "SELECT category_code FROM s_stock_category WHERE 1 "
    arr['limit'] = 15
    arr['run_script'] = "php /htdocs/soga/trader/index.php Base get_stock_dc_category %s "
    return arr


#东方财富网题材
def get_stock_10jqka_category(params):
    arr = {}
    arr['sql'] = "SELECT category_code FROM s_stock_category WHERE 1 "
    arr['limit'] = 15
    arr['run_script'] = "php /htdocs/soga/trader/index.php Base get_stock_10jqka_category %s "
    return arr


def get_xueqiu_users(params):
    arr = {}
    arr['sql'] = "SELECT s_code FROM s_stock_list WHERE 1 "
    arr['limit'] = 15
    arr['run_script'] = "php /htdocs/soga/trader/index.php XueQiu get_xq_user_from_topic %s "
    return arr


def get_xueqiu_follows_users(params):
    arr = {}
    arr['sql'] = "SELECT user_id FROM user_xq WHERE 1 "
    arr['limit'] = 15
    arr['run_script'] = "php /htdocs/soga/trader/index.php XueQiu get_xq_user_from_follows %s "
    return arr


def get_xq_title(params):
    arr = {}
    arr['sql'] = "SELECT user_id FROM user_xq WHERE original_count>30 and followers_count >=80 ORDER BY  original_count DESC"
    arr['limit'] = 15
    arr['run_script'] = "php /htdocs/soga/trader/index.php XueQiu get_xq_title %s "
    return arr

if __name__ == '__main__':
    mysql = sMysql('127.0.0.1', 'root', '1234asdf', 'stock')
    spider = SpiderEngine()
    sTools = sTools()


print sys.argv

function = eval(sys.argv[1])
params = 0
if(len(sys.argv) == 3):
    params = sys.argv[2]

#build params
script = function(params)

run_worker_template(script)

sys.exit()
