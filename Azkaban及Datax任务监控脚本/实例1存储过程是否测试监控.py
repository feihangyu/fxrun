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
        host='gz-cdb-2huk27sw.sql.tencentcdb.com', #需要换成内网ip
        port=63298,
        user='xxx',
        passwd='xxx',
        db='feods',
        charset='utf8')
    cur = conn.cursor()

    email_list = ['fezs_it_data@sf-express.com']

    try:
        sql = "SELECT t.*,c.email FROM (SELECT  a.*,b.createtime,CASE WHEN TIMESTAMPDIFF(MINUTE,a.action_datetime,b.createtime) BETWEEN 0 AND 30 THEN '30分钟内已测试' " \
              "WHEN TIMESTAMPDIFF(MINUTE,a.action_datetime,b.createtime) <0 THEN '未测试' ELSE '已测试' END AS is_test " \
              "FROM (SELECT sdate,procedure_name,maintainer,action_flag,action_datetime,notice_times  FROM feods.`prc_procedure_exe_log` " \
              "WHERE sdate=CURRENT_DATE AND action_flag='修改' ) a LEFT JOIN (SELECT task_name,MAX(createtime) AS createtime FROM feods.`sf_dw_task_log` WHERE statedate=CURRENT_DATE group by task_name ) b " \
              "ON a.procedure_name=b.task_name) t LEFT JOIN (SELECT process_name,maintainer,email FROM feods.`prc_project_process_info` " \
              "GROUP BY process_name,maintainer,email) c ON t.procedure_name=c.process_name WHERE t.is_test='未测试' and t.notice_times<2;"

        cur.execute(sql)
        res = cur.fetchall()
        if res:
            for i in res:
                procedure_name=i[1]
                maintainer=i[2]
                action_flag=i[3]
                action_datetime=i[4]
                email=i[8]
                msg = "%s,你好！你今日在实例1上 于%s %s 了你的存储过程：%s ,还未测试，还请记得及时测试，谢谢！"%(maintainer,action_datetime,action_flag,procedure_name)
                # 发送邮件到指定的维护人员
                title='实例1存储过程未测试'
                sendEmail(title, msg, [email])
                #更新邮件通知次数
                updsql="UPDATE feods.`prc_procedure_exe_log` SET notice_times=notice_times+1 WHERE procedure_name='{}' and action_datetime='{}'".format(procedure_name,action_datetime)
                cur.execute(updsql)
                conn.commit()
                #print('邮件通知次数已更新')

    except Exception as e:
        print(e)
        conn.rollback()
    finally:
        conn.commit()
        cur.close()
        #server.stop()

#每隔30分钟触发一次
process_is_test()
