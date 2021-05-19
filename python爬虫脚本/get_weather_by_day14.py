# 每日爬取货架所在城市（共59个）近14天 的天气数据

import time
from lxml import etree
import pymysql
import datetime
import requests

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36'
}

def get_week_day(date):
  week_day_dict = {
    0 : '星期一',
    1 : '星期二',
    2 : '星期三',
    3 : '星期四',
    4 : '星期五',
    5 : '星期六',
    6 : '星期日',
  }
  day = date.weekday()
  return week_day_dict[day]


def get_weather_by_day():
    # 创建数据库连接对象
    conn = pymysql.connect(
        host='xxxxx',
        port=0000,
        user='xxx',
        passwd='xxx',
        db='fe_dwd',
        charset='utf8')
    cursor = conn.cursor()

    # 从数据库获取城市信息
    city_tuple = ('',)
    try:
        sql_get_city = 'SELECT city_name,city_name_py FROM fe_dwd.dwd_city_business'
        cursor.execute(sql_get_city)
        city_tuple = cursor.fetchall()
    except Exception as e:
        print(e)

    base_url = r'https://www.tianqi.com/{}/15/'
    # print('*********************天气数据开始抓取*****************')
    info_list = []
    # proxy = { "http": "http://10.200.130.17:3128", "https": "http://10.200.130.17:3128"}  # 代理ip
    for c, cp in city_tuple:
        ct_dict = {}
        ct_dict[c] = []
        url = base_url.format(cp)
        attempts = 0
        success = False
        while attempts < 5 and not success:
            try:
                res = requests.get(url, headers=headers)  # proxies=proxy,
                etree_obj = etree.HTML(res.text)
                info_lists = etree_obj.xpath("//ul[@class='weaul']/li")
                ss_list = []
                for il in info_lists:
                    sdate = il.xpath("./a/div[1]/span[1]/text()")[0]   # 日期
                    if datetime.datetime.now().strftime('%m') == '12' and sdate[:2] == '01':  # 如果爬取的日期（不含年）sdate是01月，并且当前月份是12月，那么对应的sdate即为下一年的月份
                        sdate = str(int(datetime.datetime.now().strftime('%Y')) + 1) + '-' + sdate
                    else:
                        sdate = datetime.datetime.now().strftime('%Y') + '-' + sdate
                    sweek=get_week_day(datetime.datetime.strptime(sdate, '%Y-%m-%d'))  #星期
                    tianqi = il.xpath("./a/div[3]/text()")[0]  # 天气
                    low_temp = il.xpath("./a/div[4]/span[1]/text()")[0]  # 最低气温
                    high_temp = il.xpath("./a/div[4]/span[2]/text()")[0]  # 最高气温
                    temp_range = low_temp + '℃~' + high_temp + '℃'
                    #print(c, sdate, sweek, tianqi, temp_range, high_temp, low_temp)
                    ss_list.append((c, sdate, sweek, tianqi, temp_range, high_temp, low_temp))
                ct_dict[c].extend(ss_list)
                success = True  # 将状态设置为成功，跳出循环
                if attempts > 0 and success:
                    print('第%d次重试成功' % attempts)
            except Exception as e:
                print(e)
                attempts += 1
                print('即将进行第%d次重试' % attempts)
                time.sleep(30)  # 设置等待30秒钟重试
                if attempts == 5:
                    break
        time.sleep(1)
        info_list.append(ct_dict)
    #print(info_list)
    for j in info_list:
        cname = list(j.keys())[0]
        try:
            sql = '''
            REPLACE INTO fe_dwd.dwd_shelf_city_weather_day(city_name,sdate,sweek,weather,temp_range,high_temperature,low_temperature)
            values(%s,%s,%s,%s,%s,%s,%s)
            '''
            cursor.executemany(sql, (j.get(cname)))
            conn.commit()
            print('insert ok')
        except Exception as e:
            conn.rollback()
            print(e)
    print('*********************天气数据已入库*****************')
    cursor.close()
    conn.close()


if __name__ == '__main__':
    get_weather_by_day()
