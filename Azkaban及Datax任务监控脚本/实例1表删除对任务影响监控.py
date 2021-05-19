import pymysql
# 导入发邮件的包
import smtplib
# 需要将文本内容与附件拼接在一起  发送的内容时可变的
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import datetime
import calendar


def sendEmail(title,content, receivers):
    # Config
    smtp_server = "lsmtp.sf-express.com"
    smtp_port = 25
    mail_user = "fekj_monitor@sf-express.com"
    mail_pass = "xxxxx"  #替换成对应的密码
    sender = 'fekj_monitor@sf-express.com'
    message = MIMEMultipart()  # 声明一个可变接受内容的对象
    message["From"] = sender  # 包含发送者
    message["To"] = ",".join(receivers)  # 接收者
    message["Subject"] = title  # 主题
    text = MIMEText(content, "plain", "utf-8")  # 创建文本部分
    message.attach(text)  # 将文本内容拼接在要发送的信息中
    try:
        smtp = smtplib.SMTP()
        smtp.connect(smtp_server, smtp_port)
        smtp.ehlo()
        smtp.login(mail_user, mail_pass)
        smtp.sendmail(sender, receivers, message.as_string())
        print("邮件发送成功")
    except smtplib.SMTPException:
        print("Error: 无法发送邮件")


def createTable(conn,cursor):
    try:
        sql0="drop table if exists test.prc_table_modify_info;"
        cursor.execute(sql0)
        conn.commit()
        sql = '''
                CREATE TABLE test.prc_table_modify_info(
                pid INT PRIMARY KEY AUTO_INCREMENT COMMENT '主键id',
                table_name VARCHAR(64) COMMENT'表名',
                create_time DATETIME COMMENT '创建时间'
                ) ENGINE=INNODB DEFAULT CHARSET=utf8mb4 COMMENT='ods_dwd_dm_sserp库表最新创建信息'
            '''
        cursor.execute(sql)
        conn.commit()
        print('create table ok!')
    except Exception as e:
        print(e)


def insertData(conn,cursor):
    sql = '''
    insert into test.prc_table_modify_info(table_name,create_time)
    SELECT CONCAT(table_schema,'.',table_name) AS table_name,create_time 
    FROM `information_schema`.`TABLES` WHERE TABLE_SCHEMA IN ('feods','fe_dm','fe_dwd','sserp')
    AND table_type="BASE TABLE";'''
    cursor.execute(sql)
    conn.commit()
    print('insert ok')


def tableProcess(conn,cursor):
    try:
        sql='''
        SELECT * FROM (
        SELECT
        a.table_name AS a_name,a.create_time AS a_time,b.table_name AS b_name,b.create_time AS b_time,CASE WHEN b.table_name IS NULL THEN '删除' END AS flag
        FROM test.prc_table_modify_info a  -- 记录最近的表信息
        LEFT JOIN (SELECT CONCAT(table_schema,'.',table_name)AS table_name,table_type ,create_time 
        FROM `information_schema`.`TABLES` WHERE TABLE_SCHEMA IN ('feods','fe_dm','fe_dwd','sserp')
        AND table_type='BASE TABLE') b  -- 记录最新的表信息
        ON a.table_name=b.table_name) k WHERE k.flag='删除'
        '''
        cursor.execute(sql)
        res=cursor.fetchall()
        num=cursor.rowcount
        print('查出%d条纪录'%num)
        delete_table=[]
        msg_list=[]
        if num>0:
            for r in res:
                del_table=r[0]
                delete_table.append(del_table)
                #查询受影响的存储过程
                sql0='''
                SELECT a.project,b.update_frequency,c.maintainer,c.email
                FROM feods.`prc_project_process_source_aim_table_info` a
                LEFT JOIN (SELECT project,update_frequency FROM feods.`prc_project_relationship_info` GROUP BY project,update_frequency)b
                ON a.project=b.project
                LEFT JOIN (SELECT project,maintainer,email FROM feods.`prc_project_process_info` GROUP BY project,maintainer,email) c
                ON a.project=c.project
                WHERE CONCAT(a.source_base,'.',a.source_table)='{}'
                GROUP BY a.project;'''.format(del_table)
                cursor.execute(sql0)
                res=cursor.fetchall()
                table_list = '通过依赖关系分析，对其他azkaban调度任务没有影响'
                name_list=''
                if res:
                    table_list = ';\n'.join([(i[2] if i[2] !=None else '未知人员')+ '的' + (i[1] if i[1] != None else '未知频率')+'调度的azkaban任务：'+i[0] for i in res if i[0]!='没有找到工程名'] )
                    if table_list =='':  #以防上面table_list为空
                        table_list = '通过依赖关系分析，对其他azkaban调度任务没有影响'

                #查询网易有数抽取任务影响
                sql_wy='''
                SELECT * FROM fe_dwd.`dwd_wangyi_data_extract_info` WHERE source_table='{}' AND data_base<>'实例2'
                '''.format(del_table)
                cursor.execute(sql_wy)
                res=cursor.fetchall()
                wy_list = '通过网易有数抽取sql查询，对其他网易有数抽取任务没有影响'
                if res:
                    wy_list = '\n'.join(list(set(['将会影响：'+i[3] + '的'+i[1]+ '网易有数'+str(i[4]).split(" ")[1] +'抽取的任务：'+ i[2] for i in res])))
                    if wy_list =='':  #以防上面table_list为空
                        wy_list = '通过网易有数抽取sql查询，对其他网易有数抽取任务没有影响'
                content = "实例1表：%s 被删除，将会对以下任务有影响：\n%s\n%s" % (del_table, table_list,wy_list)
                msg_list.append(content)
        if len(msg_list)>0:
            # 发送邮件
            title = '实例1表删除影响任务告警'
            email_list = ['fezs_it_data@sf-express.com']
            sendEmail(title, '\n\n'.join(msg_list)+"\n以上，请知悉！", email_list)
            print('实例1表删除告警已通过邮件发送')
            #删除表数据，以防下次运行再次发邮件
            sql2="delete from test.prc_table_modify_info where table_name in ({})".format("'"+"','".join(delete_table)+"'")
            cursor.execute(sql2)
            conn.commit()
    except Exception as e:
        conn.rollback()
        print(e)

def main():
    #连接数据库
    conn = pymysql.connect(
        host='gz-cdb-2huk27sw.sql.tencentcdb.com', #需要换成内网ip
        port=63298,
        user='xxx',
        passwd='xxxxx',
        db='feods',
        charset='utf8')
    cursor = conn.cursor()
    # 创建数据表
    #每周一更新数据，用作旧版数据，已便与删除的进行判断
    currentdate = datetime.date.today()
    year = currentdate.year
    month = currentdate.month
    day = currentdate.day
    currentday = calendar.weekday(year, month, day)
    if currentday == 0:
        print('当日为周一，更新数据')
        createTable(conn,cursor)
        insertData(conn,cursor)
    print('开始监控')
    tableProcess(conn,cursor)
    print('结束监控')
    cursor.close()
    conn.close()


#每隔2小时触发一次
main()

