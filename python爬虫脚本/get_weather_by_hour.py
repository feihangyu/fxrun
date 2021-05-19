# 每日每隔一小时结存一次货架所在城市（共59个）的实时数据

import time
from lxml import etree
import pymysql
import datetime
import requests

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36'
}


def get_weather_by_hour():
    # 创建数据库连接对象
    conn = pymysql.connect(
        host='xxxxx',
        port=00000,
        user='xxxx',
        passwd='xxxxx',
        db='xxx',
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

    base_url = r'https://www.tianqi.com/{}/'

    proxy = { "http": "http://10.200.130.17:3128", "https": "http://10.200.130.17:3128"}  # 代理ip
    for c, cp in city_tuple:
        url = base_url.format(cp)
        print(url)
        attempts = 0
        success = False
        while attempts < 5 and not success:
            try:
                res = requests.get(url,proxies=proxy, headers=headers)  #
                etree_obj = etree.HTML(res.text)
                date_info = etree_obj.xpath("//dl[@class='weather_info']/dd[2]/text()")[0].split('　')[0]  # 日期与星期
                sdate = date_info.replace('年', '-').replace('月', '-').replace('日', '')
                sweek = etree_obj.xpath("//dl[@class='weather_info']/dd[2]/text()")[0].split('　')[1]
                current_temp = etree_obj.xpath("//p[@class='now']/b/text()")[0]  # + '℃' # 当前实时气温
                current_weather = etree_obj.xpath("//dd[@class='weather']/span/b/text()")[0]  # 当前天气
                current_wet = etree_obj.xpath("//dd[@class='shidu']/b[1]/text()")[0].split("：")[1].split("%")[0]  # 当前湿度
                current_wind = etree_obj.xpath("//dd[@class='shidu']/b[2]/text()")[0].split("：")[1]  # 当前风向
                current_zwx = etree_obj.xpath("//dd[@class='shidu']/b[3]/text()")[0].split("：")[1]  # 当前紫外线强度
                current_kongqi = etree_obj.xpath("//dd[@class='kongqi']/h5/text()")[0].split("：")[1]  # 当前空气质量
                current_pm = etree_obj.xpath("//dd[@class='kongqi']/h6/text()")[0].split(": ")[1]  # 当前PM值
                chour = datetime.datetime.now().hour  # 当前小时
                print(c,sdate,sweek,current_temp,current_wet,current_wind,current_zwx,current_kongqi,current_pm)
                success = True  # 将状态设置为成功，跳出循环
                if attempts > 0 and success:
                    print('第%d次重试成功' % attempts)
                # 写入数据库
                try:
                    sql = '''
                    REPLACE INTO fe_dwd.dwd_shelf_city_weather_day_hour(city_name,sdate,chour,sweek,current_temperature,current_weather,wind_direction,air_quality,wet,pm,zwx)
                    values('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')
                    '''.format(c, sdate, chour, sweek, current_temp, current_weather, current_wind, current_kongqi,
                               current_wet, current_pm, current_zwx)
                    cursor.execute(sql)
                    conn.commit()
                    print('insert ok')
                except Exception as e:
                    conn.rollback()
                    print(e)

            except Exception as e:
                print(e)
                attempts += 1
                print('即将进行第%d次重试' % attempts)
                time.sleep(30)  # 设置等待30秒钟重试
                if attempts == 5:
                    break
        time.sleep(0.5)

    cursor.close()
    conn.close()


if __name__ == '__main__':
    get_weather_by_hour()
