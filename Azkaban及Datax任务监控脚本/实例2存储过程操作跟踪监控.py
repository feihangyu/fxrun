from datetime import datetime, date
import pymysql


# 导入发邮件的包
import smtplib
# 需要将文本内容与附件拼接在一起  发送的内容时可变的
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

email_list = ['fezs_it_data@sf-express.com']

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
        sql0="drop table if exists test.prc_process_modify_info;"
        cursor.execute(sql0)
        conn.commit()
        sql = '''
                CREATE TABLE test.prc_process_modify_info(
                pid INT PRIMARY KEY AUTO_INCREMENT COMMENT '主键id',
                process_name VARCHAR(64) COMMENT'存储过程名',
                create_time DATETIME COMMENT '创建时间'
                ) ENGINE=INNODB DEFAULT CHARSET=utf8mb4 COMMENT='sh_process库存储过程最新创建信息'
            '''
        cursor.execute(sql)
        conn.commit()
        #print('create table ok!')
    except Exception as e:
        print(e)


def myList(conn,cursor):
    new_list = []  # 新建一个空列表用来存储元组数据
    sql = "SHOW PROCEDURE STATUS WHERE Db='sh_process'"
    cursor.execute(sql)
    res = cursor.fetchall()
    snum = cursor.rowcount
    #print(snum)
    for i in range(snum):
        process_name = res[i][1]
        create_time = res[i][4]
        tup = (process_name, create_time)
        new_list.append(tup)
    return new_list

def myInsert(newList,conn,cursor):
    try:
        sql = "insert into test.prc_process_modify_info(process_name,create_time) values(%s,%s)"
        cursor.executemany(sql, newList)
        conn.commit()
        #print('insert ok')
    except Exception as e:
        conn.rollback()
        print(e)

def detectiveProcess(conn,cursor):
    try:
        sql='''
            select * from (
            -- 查存储过程修改 删除
            SELECT 
            a.procedure_name as a_name,a.update_time as a_time,b.process_name as b_name,b.create_time as b_time,
            CASE WHEN b.process_name IS NOT NULL AND b.create_time<>a.update_time THEN '修改'
                 WHEN b.process_name IS NULL  THEN '删除'
                 END AS flag
            FROM fe_dwd.`dwd_prc_procedure_detective_info` a
            LEFT JOIN test.prc_process_modify_info b
            ON a.procedure_name=b.process_name WHERE a.delete_flag=0
            union all
            -- 查存储过程新增
            SELECT 
            a.procedure_name as a_name,a.update_time as a_time,b.process_name as b_name,b.create_time as b_time,
            CASE WHEN a.procedure_name IS  NULL  THEN '新增' 
                 END AS flag
            FROM fe_dwd.`dwd_prc_procedure_detective_info` a
            RIGHT JOIN test.prc_process_modify_info b
            ON a.procedure_name=b.process_name AND a.delete_flag=0) k
            where k.flag is not null;
        '''
        cursor.execute(sql)
        res=cursor.fetchall()
        num=cursor.rowcount
        #print('查出%d条纪录'%num)
        for r in res:
            if r[4]=='新增':
                print('有新增的存储过程**********************')
                add_sql = '''
                -- 将新增的存储过程写入数据库
                INSERT INTO fe_dwd.dwd_prc_procedure_detective_info(procedure_name,create_time,update_time)
                SELECT process_name,create_time,create_time FROM test.prc_process_modify_info WHERE process_name='{}';'''.format(r[2])
                cursor.execute(add_sql)
                conn.commit()
                print('新增存储过程已提交数据库')

                # 去数据库查询对应的维护人员信息
                sqlw = "select maintainer,email from fe_dwd.dwd_prc_project_process_info where process_name='{}'".format(r[2])
                cursor.execute(sqlw)
                conn.commit()
                res = cursor.fetchall()
                title = '实例2存储过程新增通知'
                add_maintainer = ''
                if res:
                    add_maintainer = res[0][0]
                    email = [res[0][1]]
                    content = "%s，你好！你新增的存储过程：%s 于 %s 添加到实例2，并将会进行执行测试，如有问题将会及时联系你处理，正常执行也会通知到你，请知悉！" % (
                    add_maintainer, r[2], r[3])

                else:
                    email = email_list
                    content = "组内同事，你们好！数据组于 %s 在实例2上新增存储过程：%s，并将会进行执行测试，如有问题将会及时联系你们处理，正常执行也会通知到你们，请知悉！" % (
                    r[3], r[2])
                sendEmail(title, content, email)
                print('存储过程新增通知已通过邮件发送')

                # 将操作日志信息写入数据库
                sql3e = "INSERT INTO fe_dwd.dwd_prc_procedure_exe_log(sdate,procedure_name,action_flag,loginfo) VALUES('{}','{}','新增','{}')" \
                    .format(date.today(), r[2], content)
                cursor.execute(sql3e)
                conn.commit()
                print('操作日志信息已写入数据库')
            if r[4]=='修改':
                print('有被修改的存储过程**********************')
                sql4 = '''
                    -- 更新发生修改的存储过程信息 (最近一次修改时间信息)
                    UPDATE fe_dwd.`dwd_prc_procedure_detective_info` a
                    JOIN test.prc_process_modify_info b
                    ON a.procedure_name=b.process_name
                    SET a.update_time=b.create_time
                    WHERE a.procedure_name='{}' AND a.delete_flag=0;'''.format(r[0])
                cursor.execute(sql4)
                conn.commit()
                print('存储过程修改时间信息已提交数据库')
                #查询维护人信息
                # 数据库查询维护人员及邮箱
                sql5 = "select process_name,maintainer,email from fe_dwd.dwd_prc_project_process_info " \
                       "where process_name='{}' group by process_name,maintainer,email ".format(r[0])
                cursor.execute(sql5)
                conn.commit()
                res = cursor.fetchall()
                maintainer=''
                email=[]
                if res:
                    maintainer = res[0][1]
                    email = [res[0][2]]
                # 发送邮件到指定的维护人员
                    title = '实例2存储过程修改通知'
                    content = "%s，你好！实例2上你维护的存储过程：%s 于 %s 发生过修改，并将会进行测试，如发现问题将及时告知于你，测试通过后会通知到你，请知悉！" % (
                    maintainer, r[0], r[3])
                    sendEmail(title, content, email)
                    print('存储过程修改通知已通过邮件发送')
                # 将操作日志信息写入数据库
                sql4e = "INSERT INTO fe_dwd.dwd_prc_procedure_exe_log(sdate,procedure_name,maintainer,action_flag,loginfo) VALUES('{}','{}','{}','修改','{}')" \
                    .format(date.today(), r[0], maintainer, content)
                cursor.execute(sql4e)
                conn.commit()
                print('操作日志信息已写入数据库')
            if r[4]=='删除':
                print('有被删除的存储过程**********************')
                # 将删除信息写入数据库
                print('将删除时间写入数据库')
                sql8 = "update fe_dwd.dwd_prc_procedure_detective_info set delete_time=NOW(),delete_flag=1 where procedure_name = '{}' and delete_flag=0 ".format(r[0])
                cursor.execute(sql8)
                conn.commit()
                print('存储过程删除时间信息已提交数据库')
                # 获取删除任务所影响的任务
                sql10 = "SELECT a.project,b.maintainer,CONCAT(a.update_frequency,a.start_time,'开始执行') AS frequecy,a.dependent_project " \
                        "FROM fe_dwd.dwd_prc_project_relationship_detail_info a JOIN (SELECT project,maintainer,email " \
                        "FROM fe_dwd.`dwd_prc_project_process_info` GROUP BY project,maintainer,email )b " \
                        "ON a.project=b.project WHERE a.dependent_project='{}' AND a.update_frequency != '已停止' " \
                        "ORDER BY a.start_time".format(r[0])
                cursor.execute(sql10)
                conn.commit()
                res = cursor.fetchall()
                table_list = '通过依赖关系分析，对其他任务没有影响'
                name_list = ''
                if res:
                    table_list = ';\n'.join([i[1] + '的' + i[0] + i[2] for i in res])
                    name_list = ','.join([i[1] for i in res])
                # 获取失败任务的维护人员 及邮箱
                sql7 = "select process_name,maintainer,email from fe_dwd.dwd_prc_project_process_info " \
                       "where process_name='{}' group by process_name,maintainer,email ".format(r[0])
                cursor.execute(sql7)
                conn.commit()
                res = cursor.fetchall()
                if res:
                    maintainer = res[0][1]
                    email = [res[0][2]]
                    del_time = datetime.now()
                    content = "%s，你好！实例2上你维护的存储过程：%s 于 %s 被删除，将会对以下任务有影响：\n%s\n以上，请知悉！\n如是本人操作，请忽略；非本人操作，请咨询组内人员！" % (
                        maintainer, r[0], del_time, table_list)
                else:
                    maintainer = ''
                    email = []
                    del_time = datetime.now()
                    content = "组内同事，大家好！实例2上未知人员维护的存储过程：%s 于 %s 被删除，将会对以下任务有影响：\n%s\n以上，请知悉！\n请该操作人员及时确认是否勿删，谢谢！" % (
                        r[0], del_time, table_list)
                # 发送邮件到指定的维护人员
                title = '实例2存储过程删除通知'
                sendEmail(title, content, email_list + email)
                print('存储过程删除通知已通过邮件发送')
                # 将操作日志信息写入数据库
                sql8e = "INSERT INTO fe_dwd.dwd_prc_procedure_exe_log(sdate,procedure_name,maintainer,action_flag,loginfo) VALUES('{}','{}','{}','删除','{}')" \
                    .format(date.today(), r[0], maintainer, content)
                cursor.execute(sql8e)
                conn.commit()
                print('操作日志信息已写入数据库')
        #更新feods.prc_procedure_exe_log 表的详细操作时间数据
        update_sql = "UPDATE fe_dwd.`dwd_prc_procedure_exe_log` SET action_datetime= CASE WHEN action_flag='修改' THEN LEFT(SUBSTRING_INDEX(SUBSTRING_INDEX(loginfo,'于 ',-1),'发生',1),19) " \
                     "WHEN action_flag='删除' THEN LEFT(SUBSTRING_INDEX(SUBSTRING_INDEX(loginfo,'于 ',-1),'被删除',1),19) " \
                     "WHEN action_flag='新增' THEN LEFT(SUBSTRING_INDEX(SUBSTRING_INDEX(loginfo,'于 ',-1),'新增',1),19) ELSE '' END  " \
                     "WHERE action_datetime IS NULL;"
        cursor.execute(update_sql)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print('异常原因：',e)

def main():
    # 创建数据库连接对象
    conn = pymysql.connect(
        host='gz-cdb-3hfr4o0x.sql.tencentcdb.com',   #需要换成内网ip
        port=60611,
        user='xxx',
        passwd='xxxx',
        db='fe_dwd',
        charset='utf8')
    cursor = conn.cursor()
    # 创建数据表
    createTable(conn,cursor)
    newList = myList(conn,cursor)
    myInsert(newList,conn,cursor)
    #print('开始监控')
    detectiveProcess(conn,cursor)
    #print('结束监控')
    cursor.close()
    

#每10分钟执行一次  
main()