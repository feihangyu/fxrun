
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
        #print("邮件发送成功")
    except smtplib.SMTPException:
        print("Error: 无法发送邮件")

def get_relation(conn, cursor, fproject_list, f_list):
    project_list = []
    try:
        sql = '''
        SELECT project FROM fe_dwd.`dwd_prc_project_relationship_detail_info` 
        WHERE  dependent_project IN ({}) AND update_frequency not like '%已停止%' AND priority=1 
        GROUP BY project
        '''.format("'" + "','".join(fproject_list) + "'")
        cursor.execute(sql)
        res = cursor.fetchall()
        if res:
            for k in res:
                if k[0] not in sum(f_list, []):
                    project_list.append(k[0])
        f_list.append(project_list)
        # 递归
        if project_list != []:
            get_relation(conn, cursor, project_list, f_list)
    except Exception as e:
        print(e)
        conn.rollback()
    finally:
        # 返回受影响的任务列表
        return sum(f_list, [])


def get_project_relation(conn, cursor):
    mesg_list = []
    try:
        sql = '''
                SELECT DISTINCT a.name,b.maintainer FROM fe_dwd.dwd_execution_logs_text a
                JOIN fe_dwd.dwd_prc_project_relationship_detail_info b
                ON a.name=b.project
                WHERE add_time>=SUBDATE(CURRENT_DATE,0) AND (TEXT LIKE '%failed%' OR TEXT LIKE '%FAILED%' OR TEXT LIKE '%killed%' OR TEXT LIKE '%KILLED%')  
                AND LENGTH(NAME)>0 AND TIMESTAMPDIFF(MINUTE,a.add_time,CURRENT_TIMESTAMP)<7 AND b.status='已部署' AND b.update_frequency NOT LIKE '%已停止%'
        '''
        cursor.execute(sql)
        conn.commit()
        res = cursor.fetchall()
        if res:
            for r in res:
                fail_project = r[0]  # 失败的任务
                #将失败的任务写入一个表，以便于后面修复任务后，防止多个依赖的任务同时执行导致CPU资源很高，影响任务执行
                try:
                    sql3='''
                        REPLACE INTO fe_dwd.dwd_project_fail_delay_again_execute(sdate,project,dependent_project,d_start_time)
                        SELECT 
                        CURRENT_DATE,project,dependent_project,start_time
                        FROM fe_dwd.`dwd_prc_project_relationship_detail_info` 
                        WHERE dependent_project='{}' AND STATUS='已部署' AND priority=1 AND update_frequency not like '%已停止%'
                    '''.format(fail_project)
                    cursor.execute(sql3)
                    conn.commit()

                    sql_4='''
                        -- 业务存储过程执行出错
                        INSERT INTO fe_dwd.dwd_project_execute_process_log(sdate,project,STATUS) VALUES(CURRENT_DATE,'{}','业务存储过程执行出错');
                    '''.format(fail_project)
                    cursor.execute(sql_4)
                    conn.commit()

                except Exception as e:
                    print(e)
                    conn.rollback()

                mesg="今日实例2%s维护的azkaban调度任务：%s 执行失败，请及时处理！"%(r[1],r[0])
                mesg_list.append(mesg)

                #获取该失败任务对网易有数任务的影响
                sql_wy = '''
                    SELECT * FROM fe_dwd.`dwd_wangyi_data_extract_info_base2` WHERE source_table IN (
                    SELECT CONCAT(aim_base,'.',aim_table) AS table_name FROM fe_dwd.`dwd_prc_project_process_source_aim_table_info`  WHERE project='{}'
                    AND LENGTH(aim_table)>3 GROUP BY  CONCAT(aim_base,'.',aim_table)) 
                '''.format(r[0])
                cursor.execute(sql_wy)
                res=cursor.fetchall()
                wy_list = '通过网易有数抽取sql查询，Azkaban调度失败任务：%s对网易有数抽取任务直接影响如下：\n'%(r[0])
                if res:
                    info = '\n'.join(list(set(['将会直接影响：'+i[3] + '的'+i[1]+ '网易有数的抽取任务：'+ i[2] for i in res])))
                    wy_list+=info
                else:
                    wy_list = '通过网易有数抽取sql查询，Azkaban调度失败任务：%s对网易有数抽取任务没有直接影响'%(r[0])
                mesg_list.append(wy_list)


                f_list = []
                project_affect_list = get_relation(conn, cursor, [fail_project], f_list)  # 获取调度失败的任务对其他的任务列表
                if project_affect_list != []:  # 如果没有其他间接受影响的任务
                    try:
                        sql2 = '''
                               SELECT project,maintainer,STATUS,update_frequency,start_time
                               FROM fe_dwd.`dwd_prc_project_relationship_detail_info`
                               WHERE project IN ({})
                               AND STATUS='已部署' AND update_frequency not like '%已停止%' 
                               GROUP BY project,maintainer,STATUS,update_frequency,start_time
                               ORDER BY start_time
                               '''.format("'" + "','".join(project_affect_list) + "'")
                        cursor.execute(sql2)
                        res2 = cursor.fetchall()
                        base_info = "根据依赖关系递归查找，今日实例2%s维护的azkaban调度任务：%s 执行失败，将对以下azkaban调度任务可能会产生影响：" % (r[1],r[0])
                        if res2:
                            for r2 in res2:
                                info = '\n' + r2[1] + r2[2] + r2[3] + r2[4] + '执行的azkaban调度任务：' + r2[0]
                                base_info += info
                        mesg_list.append(base_info)
                    except Exception as e:
                        print(e)
                        conn.rollback()
                else:
                    mesg2 = "根据依赖关系递归查找，今日实例2%s维护的azkaban调度任务：%s 执行失败， 对其他的azkaban任务无影响！" % (r[1],r[0])
                    mesg_list.append(mesg2)

                # 获取间接对网易有数抽取任务的影响
                if project_affect_list != []:  # 如果没有其他间接受影响的任务
                    try:
                        wy_sql = '''
                            SELECT * FROM fe_dwd.`dwd_wangyi_data_extract_info_base2` WHERE source_table IN (
                            SELECT CONCAT(aim_base,'.',aim_table) AS table_name FROM fe_dwd.`dwd_prc_project_process_source_aim_table_info`  WHERE project IN ({})
                            AND LENGTH(aim_table)>3 GROUP BY  CONCAT(aim_base,'.',aim_table)) 
                        '''.format("'" + "','".join(project_affect_list) + "'")
                        cursor.execute(wy_sql)
                        wy_sql_res2 = cursor.fetchall()
                        wy_base_info = "通过网易有数抽取sql查询，Azkaban调度失败任务：%s对网易有数任务间接影响如下：\n"%(r[0])
                        if wy_sql_res2:
                            info = '\n'.join(list(set(['将会间接影响：' + i[3] + '的' + i[1] + '网易有数的抽取任务：' + i[2] for i in wy_sql_res2])))
                            wy_base_info += info
                        mesg_list.append(wy_base_info)
                    except Exception as e:
                        print(e)
                        conn.rollback()
                else:
                    wy_mesg2 = "通过网易有数抽取sql查询，Azkaban调度失败任务：%s对网易有数抽取任务没有间接影响"%(r[0])
                    mesg_list.append(wy_mesg2)

            title = '实例2azkaban任务执行失败影响告警'
            sendEmail(title, '\n\n'.join(mesg_list) + "\n以上，请知悉！", email_list)
    except Exception as e:
        print(e)


def main():
    # 创建数据库连接对象
    conn = pymysql.connect(
        host='gz-cdb-3hfr4o0x.sql.tencentcdb.com',  #需要换成内网ip
        port=60611,
        user='xxx',
        passwd='xxx',
        db='fe_dwd',
        charset='utf8')
    cursor = conn.cursor()

    #print('开始监控************')
    get_project_relation(conn, cursor)
    #print('结束监控************')
    cursor.close()
    conn.close()

#每7分钟执行一次
main()

