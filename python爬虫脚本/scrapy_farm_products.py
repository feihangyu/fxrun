
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from lxml import etree
import time
import pymysql

def write_into_database(result):
    conn = pymysql.connect(
        host='xxxx',
        port=0000,
        user='xxxx',
        passwd='xxxxx',
        db='fe_dwd',
        charset='utf8')
    cursor = conn.cursor()
    try:
        # 插入最新的数据
        insert_sql = '''
         insert INTO fe_dwd.dwd_farm_product_info_from_government(province,company_name,product_name,type_name) values(%s,%s,%s,%s)
        '''
        cursor.executemany(insert_sql, result)
        conn.commit()
        print('insert ok')
    except Exception as e:
        conn.rollback()
        print(e)


headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36'
}
# 第i页 offset=(i-1)*20

browser_option = Options()
#browser_option.add_argument("--headless")
browser_option.add_argument("--disable-gpu")
browser_option.add_argument('user-agent=' + headers['User-Agent'])
path = r"D:\Program Files\chromedriver.exe"  # ,chrome_options=browser_option
browser = webdriver.Chrome(executable_path=path,chrome_options=browser_option)

url = "http://xffp.zgshfp.com.cn/portal/#/product-hall/product-hall-index"
browser.get(url)
time.sleep(2)
print('移动到农副加工')
farm_process = browser.find_element_by_xpath("//ul[@class='nav-tap']/li[6]")
ActionChainsDriver = ActionChains(browser).move_to_element(farm_process)
ActionChainsDriver.perform()
time.sleep(1)
#省份（陕西省、湖北省）、品类（农副加工下子项，例如肉制品、奶制品、坚果、休闲速食等）、商品名称、对应供应商
# 1：坚果 2：干果 3：干菜 4：营养滋补 5：肉制品 6：茶叶 7：休闲速食 8：其他 9：乳制品 10：酒
print('移动到细分类别')
farm_process_nut = browser.find_element_by_xpath("//ul[@class='nav-tap']/li[6]//li[9]/a")

html = browser.page_source
extree_obj = etree.HTML(html)
type_name = extree_obj.xpath("//ul[@class='nav-tap']/li[6]//li[9]/a/text()")[0]
print(type_name)

ActionChainsDriver = ActionChains(browser).click(farm_process_nut)
ActionChainsDriver.perform()
time.sleep(2)
print('移动到更多')
farm_process_more = browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div/div[2]/div/div[2]/div[2]/div[1]/div[3]/span[2]")
ActionChainsDriver = ActionChains(browser).click(farm_process_more)
ActionChainsDriver.perform()
time.sleep(2)
print('移动到湖北省')
#9：湖北省 18：陕西省 特别说明：当类别为乳制品时 对应的编号减1
farm_process_hubei = browser.find_element_by_xpath("//div[@class='area-name']/a[17]")
ActionChainsDriver = ActionChains(browser).click(farm_process_hubei)
ActionChainsDriver.perform()
time.sleep(2)

all_window_height =  []  # 创建一个列表，用于记录每一次拖动滚动条后页面的最大高度
all_window_height.append(browser.execute_script("return document.body.scrollHeight;")) #当前页面的最大高度加入列表
while True:
    browser.execute_script("scroll(0,100000)") # 执行拖动滚动条操作
    time.sleep(5)
    #wait = WebDriverWait(browser, 3)  # 浏览器等待10s
    check_height = browser.execute_script("return document.body.scrollHeight;")
    if check_height == all_window_height[-1]:  #判断拖动滚动条后的最大高度与上一次的最大高度的大小，相等表明到了最底部
        break
    else:
        all_window_height.append(check_height) #如果不想等，将当前页面最大高度加入列表。

info_list=[]
html = browser.page_source
extree_obj = etree.HTML(html)
tr_list = extree_obj.xpath("//div[@id='proList']/div")
print('共%d个产品'%(len(tr_list)-1))
for tl in tr_list[:len(tr_list)-1]:
    pname = tl.xpath("./h5/text()")[0]  #产品名
    company_name = tl.xpath("./p[1]/text()")[0]  #公司名
    province_name = tl.xpath("./p[2]/text()")[0]  #省份
    #print(pname,company_name,province_name)
    info_list.append([province_name,company_name,pname,type_name])
#写入数据库
write_into_database(info_list)

browser.close()