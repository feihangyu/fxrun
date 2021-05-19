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


def get_mapping_info():
    # 创建数据库连接对象
    conn = pymysql.connect(
        host='10.200.130.72',  # 改为内网ip
        port=3306,
        user='xxx',
        passwd='xxxx',
        db='fe_dwd',
        charset='utf8')
    cursor = conn.cursor()
    one_list = []
    two_list = []
    map_dict = {}
    datax_project_dict={}
    #erp_frequency_list=[]
    try:
        get_sql = '''
        SELECT table_name_one,table_name_two,datax_project_name,erp_frequency FROM fe_dwd.`dwd_datax_table_mapping_info` 
        WHERE delete_flag=1 AND table_name_one NOT LIKE '%feng1.%'  AND table_name_one NOT LIKE '%sserp.%' 
        AND table_name_one NOT LIKE '%feods.%' AND table_name_one NOT LIKE '%fe_dwd.%' AND table_name_one NOT LIKE '%fe_dm.%'
        AND table_name_one NOT IN (
        'fe.sf_instock_info','fe.sf_outstock_info','fe.sf_product_fill_order_item','fe.sf_warehouse_stock_info')
        '''
        cursor.execute(get_sql)
        res = cursor.fetchall()
        for r in res:
            one_list.append(r[0])
            two_list.append(r[1])
            datax_project_dict[r[1]]=r[2]+'￥'+r[3]
            #erp_frequency_list.append(r[3])
            map_dict[r[0]] = r[1]
    except Exception as e:
        print(e)
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

    return (one_list, two_list,map_dict,datax_project_dict)


def getInstanceOne(one_list):
    conn1 = pymysql.connect(
        host='10.200.130.4',  # 改为内网ip
        port=3306,
        user='xxx',
        passwd='xxx',
        db='feods',
        charset='utf8')
    cursor1 = conn1.cursor()
    one_table_dict = {}
    one_table_col_num_dict = {}
    try:
        for i in one_list:
            one_table_col_num_dict[i] = []
            base = i.split('.')[0]
            table = i.split('.')[1]
            one_table_dict[i] = []
            sql = '''
            SELECT  t.`TABLE_SCHEMA`, t.`TABLE_NAME`, t.`COLUMN_NAME`,t.`COLUMN_TYPE`, t.`COLUMN_COMMENT`
            FROM `information_schema`.`COLUMNS` t
            WHERE t.`TABLE_SCHEMA` ='{}' AND  t.`TABLE_NAME`='{}'
            '''.format(base, table)
            cursor1.execute(sql)
            res = cursor1.fetchall()
            num = cursor1.rowcount
            if res:
                one_table_col_num_dict[i].append(num)
                for r in res:
                    msg = r[2] + '+' + r[3] + '+' + r[4]
                    one_table_dict[i].append(msg)

    except Exception as e:
        print(e)
        conn1.rollback()
    finally:
        cursor1.close()
        conn1.close()
    return one_table_dict, one_table_col_num_dict


def getInstanceTwo(two_list):
    conn2 = pymysql.connect(
        host='10.200.130.72',  # 改为内网ip
        port=3306,
        user='xxx',
        passwd='xxx',
        db='fe_dwd',
        charset='utf8')
    cursor2 = conn2.cursor()
    two_table_dict = {}
    two_table_col_num_dict = {}
    try:
        for i in two_list:
            two_table_col_num_dict[i] = []
            base = i.split('.')[0]
            table = i.split('.')[1]
            two_table_dict[i] = []
            sql = '''
            SELECT  t.`TABLE_SCHEMA`, t.`TABLE_NAME`, t.`COLUMN_NAME`,t.`COLUMN_TYPE`, t.`COLUMN_COMMENT`
            FROM `information_schema`.`COLUMNS` t
            WHERE t.`TABLE_SCHEMA` ='{}' AND  t.`TABLE_NAME`='{}'
            '''.format(base, table)
            cursor2.execute(sql)
            res = cursor2.fetchall()
            num = cursor2.rowcount
            if res:
                two_table_col_num_dict[i].append(num)
                for r in res:
                    msg = r[2] + '+' + r[3] + '+' + r[4]
                    # print(msg)
                    two_table_dict[i].append(msg)
    except Exception as e:
        print(e)
        conn2.rollback()
    finally:
        cursor2.close()
        conn2.close()
    return two_table_dict, two_table_col_num_dict


def main():
    conn2 = pymysql.connect(
        host='10.200.130.72',  # 改为内网ip
        port=3306,
        user='xxx',
        passwd='xxxx',
        db='fe_dwd',
        charset='utf8')
    cursor2 = conn2.cursor()
    one_list, two_list,map_dict,datax_project_list = get_mapping_info()
    one_table_dict, one_table_col_num_dict = getInstanceOne(one_list)
    two_table_dict, two_table_col_num_dict = getInstanceTwo(two_list)

    alarm_items = ''
    project_items = ''
    for t1 in one_table_dict:
        table_one = t1
        table_two=map_dict.get(table_one)
        one_table_col_collect = one_table_dict.get(table_one)
        two_table_col_collect = two_table_dict.get(table_two)
        one_table_col_info = ','.join(one_table_col_collect)
        two_table_col_info = ','.join(two_table_col_collect)
        nwe_diff = set(one_table_col_collect) - set(two_table_col_collect)
        origin_diff = set(two_table_col_collect) - set(one_table_col_collect)
        if nwe_diff!=origin_diff:  #如果有变化 及集合均不为空
            alarm_items += '''
                          <tr >
                            <td>%s</td>
                            <td><span style="color:#cc0000;">%s</span></td>
                            <td>%s</td>
                            <td><span style="color:#cc0000;">%s</span></td>
                            <td><span style="color:#cc0000;">%s</span></td>
                            <td><span style="color:#cc0000;">%s</span></td>
                          </tr>
                    ''' % (table_one, one_table_col_info, table_two, two_table_col_info,nwe_diff, origin_diff)

            # 查询对调度任务的影响
            project_sql = '''
            SELECT project,PROCESS,maintainer,STATUS,update_frequency,start_time FROM fe_dwd.`dwd_prc_project_relationship_detail_info`
            WHERE source_table='{}' AND update_frequency NOT LIKE '%已停止%'
            '''.format(table_two)
            cursor2.execute(project_sql)
            conn2.commit()
            result = cursor2.fetchall()
            if result:
                for re in result:
                    project = re[0]
                    maintainer = re[2]
                    status = re[3]
                    frequecy = re[4]
                    start_time = re[5]

                    project_items += '''
                                  <tr >
                                    <td>%s</td>
                                    <td><span style="color:#cc0000;">%s</span></td>
                                    <td><span style="color:#cc0000;">%s</span></td>
                                    <td><span style="color:#cc0000;">%s</span></td>
                                    <td><span style="color:#cc0000;">%s</span></td>
                                    <td><span style="color:#cc0000;">%s</span></td>
                                  </tr>
                            ''' % (table_two, project, maintainer, status, frequecy, start_time)
                    print('A********:',table_one, table_two, project)

            dx=datax_project_list.get(table_two).split('￥')[0]
            ef = datax_project_list.get(table_two).split('￥')[1]
            project_items += '''
                          <tr >
                            <td>%s</td>
                            <td><span style="color:#cc0000;">%s</span></td>
                            <td><span style="color:#cc0000;">%s</span></td>
                            <td><span style="color:#cc0000;">%s</span></td>
                            <td><span style="color:#cc0000;">%s</span></td>
                            <td><span style="color:#cc0000;">%s</span></td>
                          </tr>
                    ''' % (table_two, dx+'(Datax任务)', '唐进', '已部署', ef, '去实例2fe_dwd.dwd_datax_project_sync_info查')
            print('B********:',table_one, table_two, dx)
    alarm_output = '''
                <html>
                  <body>
                    <table border="1" cellspacing="0" cellpadding="0">
                      <tr bgcolor="yellowgreen" align="center">
                        <th>实例1开发表名</th>
                        <th>实例1开发表字段集</th>
                        <th>实例2对应表名</th>
                        <th>实例2对应表字段集</th>
                        <th>实例1的差异</th>
                        <th>实例2的差异</th>
                      </tr>
                %s
                    </table>
                  </body>
                </html>
                ''' % alarm_items

    alarm_project_output=''
    if project_items != '':
        alarm_project_output = '''
                    <html>
                      <body>
                        <table border="1" cellspacing="0" cellpadding="0">
                          <tr bgcolor="yellowgreen" align="center">
                            <th>实例2对应表字段集</th>
                            <th>工程名（默认Azkaban，Datax有说明)</th>
                            <th>维护人</th>
                            <th>状态</th>
                            <th>执行频率</th>
                            <th>执行时间</th>
                          </tr>
                    %s
                        </table>
                      </body>
                    </html>
                    ''' % project_items
    #else:
    #    alarm_project_output+='\n经检查，对实例2Azkaban调度任务无影响！\n请数据组维护Datax同步人员及时将以上开发表变更信息（比如新增的字段，字段长度（重点），字段注释等）维护到实例2对应的表中，确保信息的一致，谢谢！'  # 如果有影响就发邮件
    if alarm_items != '':  #表示有表信息发生变更，就发邮件
        title = '开发表字段信息变更对azkaban任务影响告警'
        email_list = ';'.join(recipients)
        mesg_info = '开发表字段信息变更如下：\n' + alarm_output + '\n 对以下Azakaban调度任务可能有影响：\n' + alarm_project_output+'\n请数据组维护Datax同步人员及时将以上开发表变更信息（比如新增的字段，字段长度（重点），字段注释等）维护到实例2对应的表和同步任务json文件中，确保信息的一致，谢谢！'
        sendemail(email_list, title, mesg_info)

main()
