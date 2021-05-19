
import pymysql
import smtplib
# 需要将文本内容与附件拼接在一起  发送的内容时可变的
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def sendEmail(title,content, receivers):
    # Config
    smtp_server = "lsmtp.sf-express.com"
    smtp_port = 25
    mail_user = "xxxxxxx"
    mail_pass = "xxxxx"
    sender = 'xxxx'
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


def main():
    # print("开始连接实例1****************")
    conn1 = pymysql.connect(
        host='xxxx',
        port=0000,
        user='xxx',
        passwd='xxxxx',
        db='xxx',
        charset='utf8')
    cursor1 = conn1.cursor()

    mesg_list=[]
    try:
        delete_sql='delete from fe_dwd.dwd_monitor_develop_table_column_change_info where flag=2;'
        insert_sql='''
        INSERT INTO fe_dwd.dwd_monitor_develop_table_column_change_info(table_name,column_info_collect,flag)
        SELECT CONCAT(t.`TABLE_SCHEMA`,'.', t.`TABLE_NAME`) , GROUP_CONCAT(CONCAT(t.`COLUMN_NAME`,'+',t.`COLUMN_TYPE`,'+', t.`COLUMN_COMMENT`))
        ,2 AS flag
        FROM `information_schema`.`COLUMNS` t
        WHERE t.`TABLE_SCHEMA` NOT IN ('feods','fe_dwd','fe_dm','sserp','information_schema','mysql','azkaban','test','dw_history','fe_monitor','fe_history','fe_history','fe_monitor')
        GROUP BY CONCAT(t.`TABLE_SCHEMA`,'.', t.`TABLE_NAME`) ;
        '''
        select_sql='''
        SELECT 
        a.table_name,
        a.column_info_collect AS newest_column_info_collect,
        b.column_info_collect AS original_column_info_collect 
        FROM (SELECT * FROM fe_dwd.dwd_monitor_develop_table_column_change_info WHERE flag=2) a  -- 最新以防有新增表，所以作为主表
        LEFT JOIN (SELECT * FROM fe_dwd.dwd_monitor_develop_table_column_change_info WHERE flag=1) b  -- 原有信息
        ON a.table_name=b.table_name
        WHERE a.column_info_collect<>b.column_info_collect;
        '''
        cursor1.execute(delete_sql)
        conn1.commit()
        cursor1.execute(insert_sql)
        conn1.commit()
        cursor1.execute(select_sql)
        conn1.commit()
        res=cursor1.fetchall()
        # print(res)
        if res:
            for r in res:
                table_name=r[0]
                newest_column_info_collect=r[1].split(',')
                original_column_info_collect=r[2].split(',')
                mesg1='开发表：%s 字段信息有变更:\n最新的是：%s \n之前的是：%s'%(table_name,','.join(newest_column_info_collect),','.join(original_column_info_collect))
                mesg2="两者的差异：\n最新的是：%s \n原来的是：%s"%(set(newest_column_info_collect) - set(original_column_info_collect),set(original_column_info_collect) - set(newest_column_info_collect))
                mesg_list.append(mesg1)
                mesg_list.append(mesg2)
                #查询对调度任务的影响
                project_sql='''
                SELECT project,PROCESS,maintainer,STATUS,update_frequency FROM feods.`prc_project_relationship_detail_info` 
                WHERE source_table='{}' AND update_frequency NOT LIKE '%已停止%'
                '''.format(table_name)
                cursor1.execute(project_sql)
                conn1.commit()
                result = cursor1.fetchall()
                if result:
                    for re in result:
                        project=re[0]
                        maintainer=re[2]
                        STATUS=re[3]
                        update_frequency=re[4]
                        mesg='因开发表：%s 字段信息的变更，将可能会对实例1 维护人：%s，状态：%s，执行频率：%s 的任务：%s 产生影响，请及时核查处理！'%(table_name,maintainer,STATUS,update_frequency,project)
                        # print(mesg)
                        mesg_list.append(mesg)
                else:
                    mesg='开发表：%s 字段信息的变更，对实例1调度任务无影响！'%(table_name)
                    mesg_list.append(mesg)
            # 查出记录后，对表数据进行更新，删除flag=1的数据旧数据，将flag=2的最新的数据，修改为flag=1的数据
            delete_sql2='delete from fe_dwd.dwd_monitor_develop_table_column_change_info where flag=1;'
            update_sql='update fe_dwd.dwd_monitor_develop_table_column_change_info set flag =1 where flag=2;'
            cursor1.execute(delete_sql2)
            conn1.commit()
            cursor1.execute(update_sql)
            conn1.commit()

    except Exception as e:
        print(e)
        conn1.rollback()
    finally:
        cursor1.close()
        conn1.close()
    # print("已经关闭实例1连接****************")
    # print(mesg_list)
    email_list = ['xxxx']
    if mesg_list !=[]:
        title = '开发表字段信息变更对azkaban任务影响告警'
        sendEmail(title, '\n'.join(mesg_list) + "\n以上，请知悉！", email_list)

if __name__=='__main__':
    main()