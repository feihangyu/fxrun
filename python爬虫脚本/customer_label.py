
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
import time
import pymysql
import requests
import json
from tqdm import tqdm


# 创建数据库连接对象
conn = pymysql.connect(
    host='xxxx',
    port=00000,
    user='xxxx',
    passwd='xxxxxx',
    db='fe_dwd',
    charset='utf8')
cursor = conn.cursor()

phone_tuple = ('',)
try:
    #sql_get_phone = 'SELECT DISTINCT(mobile_phone) FROM test.`user_phone_yuanqi2019_huimin` WHERE LENGTH(mobile_phone)=11 AND user_id>=3000000 AND  user_id<4500000 '
    sql_get_phone='''
    SELECT DISTINCT a.mobile_phone FROM test.`user_phone_yuanqi2019_huimin` a
    LEFT JOIN test.user_phone_yuanqi_tj b
    ON a.mobile_phone=b.mobile_phone 
    WHERE  a.user_id>=4600000 AND b.`mobile_phone` IS NULL
    '''
    cursor.execute(sql_get_phone)
    phone_tuple = cursor.fetchall()
except Exception as e:
    print(e)

nums=len(phone_tuple)
#转为生成器 节省内存
phone_tuple=iter(phone_tuple)
cursor.close()
conn.close()

#print('phone_tuple:',phone_tuple)

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36'
}

browser_option = Options()
browser_option.add_argument("--headless")
browser_option.add_argument("--disable-gpu")
browser_option.add_argument('user-agent=' + headers['User-Agent'])
path = r"D:\Program Files\chromedriver.exe"  # ,chrome_options=browser_option
browser = webdriver.Chrome(executable_path=path)

url = "https://cas.sf-express.com/cas/login?service=http://profilefrontend-inc-bupp-core.dcn2.k8s.sf-express.com/cas/login?internetType=inner"  # 登录页面
browser.get(url)

in_user = browser.find_element_by_xpath("//input[@id='username']")
in_user.send_keys("xxxx")
in_password = browser.find_element_by_xpath("//input[@id='password']")
in_password.send_keys("**********")
#获取验证码
# code_url = 'https://cas.sf-express.com/cas/imgcode?service=http://profilefrontend-inc-bupp-core.dcn2.k8s.sf-express.com/cas/login?internetType=inner&a=Math.random()'
# res = requests.get(code_url, headers=headers)
# with open("icode.png", "wb") as fp:
#     fp.write(res.content)

zgmcode = input("请输入验证码：")
in_password = browser.find_element_by_xpath("//input[@id='verifyCode']")
in_password.send_keys(zgmcode)

btn = browser.find_element_by_xpath("//a[@class='login-button']")
btn.click()

#time.sleep(5)  #设置20秒钟的等待时间，让页面加载完成
JSESSIONID=input("JSESSIONID(按F2进入前端页面，找到application的Cookies)：")
sticky=input("sticky(按F2进入前端页面，找到application的Cookies)：：")
Cookie= 'sticky='+sticky+' JSESSIONID='+JSESSIONID
print('Cookies:',Cookie)


time.sleep(3)
print('移动到统计按钮点击')
statistic = browser.find_element_by_xpath("//span[@class='submenu-title-wrapper']")
ActionChainsDriver = ActionChains(browser).click(statistic)
ActionChainsDriver.perform()
time.sleep(3)

print('移动到统计按钮点击')
#statistic = browser.find_element_by_xpath("//span[@class='submenu-title-wrapper']")
statistic = browser.find_element_by_xpath("//ul[@id='/label$Menu']/li[3]")
ActionChainsDriver = ActionChains(browser).click(statistic)
ActionChainsDriver.perform()
time.sleep(3)


info_list=[]
request_url = "http://profilefrontend-inc-bupp-core.dcn2.k8s.sf-express.com/platform/userprofile/info"
#request_url='http://profilefrontend-inc-bupp-core.dcn2.k8s.sf-express.com/platform/tag/2/list'
# #request_url='http://profilefrontend-inc-bupp-core.dcn2.k8s.sf-express.com/#/label/search'
#以下仅对单一用户查询
for p,tq,r in zip(phone_tuple,tqdm(range(nums)),range(nums)):
    phone=p[0]
    payloadData = {
        'type': '2',
        'value': str(phone),
        'outputTags': ['age_level','sex']  #此处根据标签名进行设置需要获取的标签
    }

    payloadHeader={
        "Origin": "http://profilefrontend-inc-bupp-core.dcn2.k8s.sf-express.com",
        "Referer": "http://profilefrontend-inc-bupp-core.dcn2.k8s.sf-express.com/",
        "Content-Type": "application/json;charset=UTF-8",
        "Cookie":Cookie,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36'
    }

    #以防出错
    attempts = 0
    success = False
    while attempts < 5 and not success:
        try:
            dumpJsonData = json.dumps(payloadData)
            res = requests.post(request_url, data=dumpJsonData, headers=payloadHeader)
            hjson=json.loads(res.content.decode())
            json_date = hjson['data']
            if json_date:
                sex=json_date.get('sex','未知')
                age_level = json_date.get('age_level','未知')
            else:
                sex='未知'
                age_level = '未知'
            info_list.append([phone,sex,age_level])
            #time.sleep(0.01)  #每隔0.1秒钟查询一次
            success = True  # 将状态设置为成功，跳出循环
            if attempts > 0 and success:
                print('第%d次重试成功' % attempts)

        except Exception as e:
            print(e)
            attempts += 1
            print('即将进行第%d次重试' % attempts)
            time.sleep(2)  # 设置等待1秒钟重试
            if attempts == 5:
                break
    #插入数据
    if len(info_list)==10000 and r != nums-1:  #每次达到2000个就插入数据库但是没有结束
        #插入数据
        try:
            conn = pymysql.connect(
                host='xxxxx',
                port=00000,
                user='xxxx',
                passwd='xxxx',
                db='fe_dwd',
                charset='utf8')
            cursor = conn.cursor()
            sql = '''
            replace INTO test.user_phone_yuanqi_tj(mobile_phone,sex,age_level) values(%s,%s,%s)
            '''
            cursor.executemany(sql, info_list)
            conn.commit()
            info_list.clear() #重新清空列表
            print('insert ok')
            cursor.close()
            conn.close()
            #time.sleep(1)
        except Exception as e:
            conn.rollback()
            print(e)
    elif len(info_list)!=10000 and r == nums-1:  #结束的时候列表没有达到2000个也插入数据库
        #插入数据
        try:
            conn = pymysql.connect(
                host='xxxxxxx',
                port=00000,
                user='xxxx',
                passwd='xxxxx',
                db='fe_dwd',
                charset='utf8')
            cursor = conn.cursor()
            sql = '''
            replace INTO test.user_phone_yuanqi_tj(mobile_phone,sex,age_level) values(%s,%s,%s)
            '''
            cursor.executemany(sql, info_list)
            conn.commit()
            info_list.clear() #重新清空列表
            print('insert ok')
            cursor.close()
            conn.close()
            #time.sleep(1)
        except Exception as e:
            conn.rollback()
            print(e)


