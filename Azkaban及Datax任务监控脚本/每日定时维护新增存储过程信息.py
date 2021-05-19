import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pymysql
import datetime
import calendar
import requests
import json
import sys
from sendmail import *


recipients = [
                    'fezs_it_data@sf-express.com',
                    'fezs_it_yw@sf-express.com',                  
                    '1137980851@qq.com',
                    '843653957@qq.com',
                    '2457962484@qq.com',
                    '1781829326@qq.com',
                    '83031881@qq.com',
                    'tongkui.zhuang@foxmail.com',
                    'xinghuazhu@sfmail.sf-express.com',
                    'chuifangli@sfmail.sf-express.com',
                    'yunfengtang@sfmail.sf-express.com',
                ]


def main():
    conn1 = pymysql.connect(
        host='10.200.130.4',  # 改为内网ip
        port=3306,
        user='xxx',
        passwd='xxx',
        db='feods',
        charset='utf8')
    cursor1 = conn1.cursor()
    try:
        sql1 = '''
            REPLACE INTO feods.`prc_project_process_info`(project,process_name,maintainer,email) 
        SELECT a.task_name AS project ,a.task_name AS process_name,IFNULL(c.member_name,d.member_name) AS maintainer ,IFNULL(c.email,d.email) AS email
        FROM feods.`sf_dw_task_log` a
        LEFT JOIN feods.`prc_project_process_info` b 
        ON a.task_name=b.`process_name`
        LEFT JOIN fe_dwd.`dwd_data_group_member_email` c
        ON SUBSTRING_INDEX(a.loginfo,'@',1)=c.member_name_py
        LEFT JOIN fe_dwd.`dwd_data_group_member_email` d
        ON SUBSTRING_INDEX(a.loginfo,'@',1)=d.member_name
        WHERE a.start_time>=SUBDATE(CURDATE(),0) 
        AND b.process_name IS NULL
        GROUP BY task_name,SUBSTRING_INDEX(loginfo,'@',1);
            '''
        cursor1.execute(sql1)
        conn1.commit()
    except Exception as e:
        print(e)
        conn1.rollback()
    finally:
        cursor1.close()
        conn1.close()

    conn2 = pymysql.connect(
        host='10.200.130.72',  # 改为内网ip
        port=3306,
        user='xxx',
        passwd='xxxx',
        db='fe_dwd',
        charset='utf8')
    cursor2 = conn2.cursor()
    try:
        sql2='''
        REPLACE INTO fe_dwd.`dwd_prc_project_process_info`(project,process_name,maintainer,email) 
    SELECT a.task_name AS project ,a.task_name AS process_name,IFNULL(c.member_name,d.member_name) AS maintainer ,IFNULL(c.email,d.email) AS email
    FROM fe_dwd.`dwd_sf_dw_task_log` a
    LEFT JOIN fe_dwd.`dwd_prc_project_process_info` b 
    ON a.task_name=b.`process_name`
    LEFT JOIN fe_dwd.`dwd_data_group_member_email` c
    ON SUBSTRING_INDEX(a.loginfo,'@',1)=c.member_name_py
    LEFT JOIN fe_dwd.`dwd_data_group_member_email` d
    ON SUBSTRING_INDEX(a.loginfo,'@',1)=d.member_name
    WHERE a.start_time>=SUBDATE(CURDATE(),0) 
    AND b.process_name IS NULL AND a.task_name NOT LIKE '%project_relation_delay%'
    GROUP BY task_name,SUBSTRING_INDEX(loginfo,'@',1);
        '''
        cursor2.execute(sql2)
        conn2.commit()
    except Exception as e:
        print(e)
        conn2.rollback()
    finally:
        cursor2.close()
        conn2.close()

#每隔2分钟执行一次
main()
