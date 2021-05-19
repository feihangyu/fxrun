from datetime import datetime,date
import pymysql
import warnings
warnings.filterwarnings("ignore")

# 导入发邮件的包
import smtplib
#需要将文本内容与附件拼接在一起  发送的内容时可变的
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


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
        #print("邮件发送成功")
    except smtplib.SMTPException:
        print("Error: 无法发送邮件")

def process_is_test():
    # 创建数据库连接对象
    conn = pymysql.connect(
        host='gz-cdb-3hfr4o0x.sql.tencentcdb.com',  #需要换成内网ip
        port=60611,
        user='xxx',
        passwd='xxxxxm',
        db='fe_dwd',
        charset='utf8')
    cur = conn.cursor()

    email_list = ['fezs_it_data@sf-express.com']

    try:
        sql = "SELECT t.*,c.email,c.maintainer as c_maintainer FROM (SELECT  a.*,b.start_time,b.run_time,CASE WHEN TIMESTAMPDIFF(SECOND,a.action_datetime,IFNULL(b.start_time,a.action_datetime)) <=0  " \
              "THEN '未测试' ELSE '已测试' END AS is_test FROM (SELECT sdate,procedure_name,maintainer,action_flag,MAX(action_datetime) AS action_datetime " \
              "FROM fe_dwd.`dwd_prc_procedure_exe_log` WHERE sdate=CURRENT_DATE AND action_flag in ('修改','新增') and mesg_is_send is null GROUP BY sdate,procedure_name,maintainer,action_flag) a LEFT JOIN " \
              "(SELECT task_name,MAX(start_time) AS start_time,MAX(end_time) AS end_time,TIMESTAMPDIFF(SECOND,MAX(start_time),MAX(end_time)) " \
              "AS run_time FROM fe_dwd.`dwd_sf_dw_task_log` WHERE statedate=CURRENT_DATE GROUP BY task_name ) b " \
              "ON a.procedure_name=b.task_name) t LEFT JOIN (SELECT process_name,maintainer,email FROM fe_dwd.`dwd_prc_project_process_info`  " \
              "GROUP BY process_name,maintainer,email) c ON t.procedure_name=c.process_name WHERE t.is_test='已测试';"

        cur.execute(sql)
        res = cur.fetchall()
        if res:
            for i in res:
                procedure_name=i[1]
                maintainer=i[9]
                action_flag=i[3]
                action_datetime=i[4]
                start_time=i[5]
                run_time=i[6]
                email=i[8]
                msg = "%s,你好！你在实例2上维护的存储过程：%s 于%s 发生过%s ,该存储过程于 %s 开始测试，耗时：%d 秒钟，测试成功，请知悉，谢谢！"%(maintainer,procedure_name,action_datetime,action_flag,start_time,run_time)
                title='实例2存储过程测试通知'
                sendEmail(title, msg,[email])

                #修改sh_process库存储过程操作日志信息表发送通知字段
                update_sql="update fe_dwd.dwd_prc_procedure_exe_log set mesg_is_send='已发送' where procedure_name='{}' and action_flag='{}' and sdate=CURRENT_DATE ".format(procedure_name,action_flag)
                cur.execute(update_sql)
                conn.commit()
                #print('字段已更新')
    except Exception as e:
        print(e)
        conn.rollback()
    finally:
        conn.commit()
        cur.close()
        #server.stop()

#每隔10分钟执行一次
process_is_test()
