# -*- coding: utf-8 -*-
from quant.core.Stats import *
from pylsy import pylsytable


class CommonStats(StatsEngine):
    '''
    复权后的均线计算
    '''
    def __init__(self):
        StatsEngine.__init__(self)

    def demo(self):
        _dlist = self.mysql.getRecord("select * from s_stock_fenbi where s_code='sz002552' and dateline=20161208")
        indata = {
            'B': 0,
            'S': 0,
            'M': 0
            }
        ax = 0
        for i in range(0, len(_dlist)):
            item = _dlist[i]

            #if item['s_type'] == 'M' and item['s_volume'] < 30:
                #continue
            if item['s_money'] < 300000:
                continue
            ax += 1
            if item['s_type'] == 'B':
                indata[item['s_type']] += item['s_money']
            else:
                indata[item['s_type']] -= item['s_money']

        print indata
        print ax

    def _format_money(self, m):
        res = 0
        if m > 0:
            res = m/10000
            res = int(res)
            #res = '%.2f' % res
        return res

    def show_daily_lhb(self):
        s_code = self.change_scode(sys.argv[2])
        start_date = 20150610
        show_table_max = 4
        x = self.mysql.getRecord("SELECT a.*,b.codex,b.name FROM  `s_lhb_days_detail` as a left join s_lhb as b on a.yyb_id=b.codex WHERE a.s_code='%s' and dateline >=%s order by dateline desc " % (s_code, start_date))
        if len(x):
            run_day = []
            daily_yyb = {}
            daily_yyb2 = {}
            for a in range(0, len(x)):
                key = "D%s" % x[a]['dateline']
                if key not in run_day:
                    run_day.append(key)
                if x[a]['name'] is None:
                    x[a]['name'] = "YYB-%s" % x[a]['yyb_id']
                if x[a]['name'] not in daily_yyb.keys():
                    daily_yyb[x[a]['name']] = []
                daily_yyb[x[a]['name']].append(x[a])

                _name_key = "D%s|%s" % (x[a]['dateline'], x[a]['name'])
                if _name_key not in daily_yyb2.keys():
                    daily_yyb2[_name_key] = []
                daily_yyb2[_name_key].append(x[a])
            _title = ['N']
            for d in range(0, len(run_day)):
                _title.append(str(run_day[d]))
            print "\t".join(_title)
            for _name, v in daily_yyb.items():
                _name2 = _name.replace('证券股份有限公司', '')
                _name2 = _name2.replace('证券有限公司', '')
                _name2 = _name2.replace('证券有限责任公司', '')
                _name2 = _name2.replace('证券营业部', '')
                _name2 = _name2.replace('（山东）有限责任公司', '')

                #显示最近5次有龙虎榜
                is_show_name = 0
                #is_max_day = 0
                BS_str = []
                for d in range(0, len(run_day)):
                    _name_key = "%s|%s" % (run_day[d], _name)
                    #print _name_key
                    AK = ''
                    if _name_key in daily_yyb2:
                        AK = 'A'
                        is_show_name = 1
                    BS_str.append(AK)
                if is_show_name:
                    print "%s\t\t\t%s" % (_name2, "\t".join(BS_str))

        '''
        if len(x):
            run_day = [1]
            daily_yyb = {}
            daily_yyb2 = {}
            for a in range(0, len(x)):
                key = "D%s" % x[a]['dateline']
                if key not in run_day:
                    run_day.append(key)

                if x[a]['name'] is None:
                    x[a]['name'] = "YYB-%s" % x[a]['yyb_id']
                if x[a]['name'] not in daily_yyb.keys():
                    daily_yyb[x[a]['name']] = []
                daily_yyb[x[a]['name']].append(x[a])
                _name_key = "D%s|%s" % (x[a]['dateline'], x[a]['name'])
                if _name_key not in daily_yyb2.keys():
                    daily_yyb2[_name_key] = []
                daily_yyb2[_name_key].append(x[a])

            #for d in range(0, len(run_day)):
            #print "\t".join(run_day)

            for _name, v in daily_yyb.items():
                _name2 = _name.replace('证券股份有限公司', '')
                _name2 = _name2.replace('证券有限公司', '')
                _name2 = _name2.replace('证券有限责任公司', '')
                _name2 = _name2.replace('证券营业部', '')
                _name2 = _name2.replace('（山东）有限责任公司', '')

                #显示最近5次有龙虎榜
                #is_show_name = 0
                is_max_day = 0
                BS_str = ''
                for d in range(0, len(run_day)):
                    _name_key = "%s|%s" % (run_day[d], _name)
                    print _name_key
                    if _name_key in daily_yyb2:
                        BS_str = 111

                        is_max_day = 1
                        BS_strA = "%s%s=%s=%s" % (daily_yyb2[_name_key][0]['type'], daily_yyb2[_name_key][0]['s_sort'], self._format_money(daily_yyb2[_name_key][0]['B_volume']), self._format_money(daily_yyb2[_name_key][0]['S_volume']))
                        BS_strB = ''
                        if len(daily_yyb2[_name_key]) > 1:
                            BS_strB = "%s%s=%s=%s" % (daily_yyb2[_name_key][1]['type'], daily_yyb2[_name_key][1]['s_sort'], self._format_money(daily_yyb2[_name_key][1]['B_volume']), self._format_money(daily_yyb2[_name_key][1]['S_volume']))
                        BS_str = u"%s=%s==" % (BS_strA, BS_strB)

                #连续日期存在
                if is_max_day == 1:
                    print "%s\t%s" % (_name2, BS_str)
            '''

    def show_daily_lhb2(self):
    #显示个股历史龙虎数据
        s_code = self.change_scode(sys.argv[2])
        start_date = 20150610
        show_table_max = 4
        x = self.mysql.getRecord("SELECT a.*,b.codex,b.name FROM  `s_lhb_days_detail` as a left join s_lhb as b on a.yyb_id=b.codex WHERE  a.s_code='%s' and dateline >=%s order by dateline desc " % (s_code, start_date))
        if len(x):
            #先按天归类
            daily_yyb = {}
            daily_yyb2 = {}
            attributes = ["ID", "Name", "Num"]
            table_attr = ["Name"]

            tdheader = ['<tr height="40"><th width="60" bgColor="#CCCCCC" align="center">ID</th>', '<th width="100" bgColor="#CCCCCC" align="center">Name</th>', '<th width="30" bgColor="#CCCCCC" align="center">Num</th>']

            for a in range(0, len(x)):
                key = "D%s" % x[a]['dateline']
                if key not in attributes:
                    attributes.append(key)
                    tdheader.append('<th width="120" bgColor="#CCCCCC" align="center">%s</th>' % key)
                    if len(table_attr) < show_table_max:
                        table_attr.append(key)

                if x[a]['name'] is None:
                    x[a]['name'] = "YYB-%s" % x[a]['yyb_id']
                if x[a]['name'] not in daily_yyb.keys():
                    daily_yyb[x[a]['name']] = []

                daily_yyb[x[a]['name']].append(x[a])
                _name_key = "D%s|%s" % (x[a]['dateline'], x[a]['name'])
                if _name_key not in daily_yyb2.keys():
                    daily_yyb2[_name_key] = []
                daily_yyb2[_name_key].append(x[a])

            tdheader.append('</tr>')

            name_list = []
            table_td_list = {}
            show_table = pylsytable(table_attr)

            #print "====".join(tdheader)
            #sys.exit()
            all_tr_list = []
            for _name, v in daily_yyb.items():
                #制作每一行数据
                _name2 = _name.replace('证券股份有限公司', '')
                _name2 = _name2.replace('证券有限公司', '')
                _name2 = _name2.replace('证券有限责任公司', '')
                _name2 = _name2.replace('证券营业部', '')
                _name2 = _name2.replace('（山东）有限责任公司', '')

                #显示最近5次有龙虎榜
                #is_show_name = 0
                for d in range(1, len(table_attr)):
                    _name_key = "%s|%s" % (table_attr[d], _name)

                    if table_attr[d] not in table_td_list.keys():
                        table_td_list[table_attr[d]] = []

                    BS_str = ''
                    if _name_key in daily_yyb2:
                        #is_show_name = 1
                        BS_strA = "%s%s=%s=%s" % (daily_yyb2[_name_key][0]['type'], daily_yyb2[_name_key][0]['s_sort'], self._format_money(daily_yyb2[_name_key][0]['B_volume']), self._format_money(daily_yyb2[_name_key][0]['S_volume']))
                        BS_strB = ''
                        if len(daily_yyb2[_name_key]) > 1:
                            BS_strB = "%s%s=%s=%s" % (daily_yyb2[_name_key][1]['type'], daily_yyb2[_name_key][1]['s_sort'], self._format_money(daily_yyb2[_name_key][1]['B_volume']), self._format_money(daily_yyb2[_name_key][1]['S_volume']))
                        BS_str = u"%s=%s==" % (BS_strA, BS_strB)
                    table_td_list[table_attr[d]].append(BS_str)

                #print "%s==%s" % (_name2, is_show_name)
                #if is_show_name == 1:
                name_list.append(_name2)

                tr_data = ["%s" % v[0]['yyb_id'], _name2, 0]
                #周期内出现的天数
                _has_num = 0
                for b in range(3, len(attributes)):
                    _name_key = "%s|%s" % (attributes[b], _name)

                    BS_str = ''
                    if _name_key in daily_yyb2:
                        _has_num += 1
                        #营业部做T
                        BS_strA = "%s%s=%s=%s" % (daily_yyb2[_name_key][0]['type'], daily_yyb2[_name_key][0]['s_sort'], self._format_money(daily_yyb2[_name_key][0]['B_volume']), self._format_money(daily_yyb2[_name_key][0]['S_volume']))
                        BS_strB = ''
                        if len(daily_yyb2[_name_key]) > 1:
                            BS_strB = "%s%s=%s=%s" % (daily_yyb2[_name_key][1]['type'], daily_yyb2[_name_key][1]['s_sort'], self._format_money(daily_yyb2[_name_key][1]['B_volume']), self._format_money(daily_yyb2[_name_key][1]['S_volume']))
                        BS_str = u"%s\n%s==\n" % (BS_strA, BS_strB)
                    tr_data.append(BS_str)

                tr_data[2] = _has_num

                _td_str = ['<tr height="40">']
                for c in range(0, len(tr_data)):
                    _td_str.append('<td bgColor="#FFFFFF" align="center">%s</td>' % tr_data[c])
                _td_str.append('</tr>')

                all_tr_list.append("\n".join(_td_str))

        if len(sys.argv) == 3:
            table = '<html><head><meta http-equiv="Content-Type" content="text/html; charset=UTF-8" /><title></title></head><body>\n\
                <table border="1" width="1024">%s\n%s</table></body></html>' % ("\n".join(tdheader), "\n".join(all_tr_list))
            print table
        else:
            for b in range(1, len(table_attr)):
                show_table.add_data(table_attr[b], table_td_list[table_attr[b]])
            #print name_list
            show_table.add_data("Name", name_list)
            print(show_table.__str__())
