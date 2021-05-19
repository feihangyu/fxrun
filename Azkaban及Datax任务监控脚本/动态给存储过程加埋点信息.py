


#动态给每个存储过程 insert into / replace into / update

# 从映射表读取存储过程 与 维护人员的信息
import pandas as pd
import os
import re

fpath=r'd:\user\01387858\desktop\存储过程\动态添加'
wpath=r'd:\user\01387858\desktop\存储过程\动态添加结果'
files=os.listdir(fpath)
# 匹配 insert
insert_re=re.compile(r'.*(INSERT|insert).*')
#replace_re=re.compile(r'.*[REPLACE|replace].*')
update_re=re.compile(r'.*(UPDATE|update).*')
create_re=re.compile(r'.*(CREATE|create).*(TEMPORARY|temporary).*')

def get_log(sql_str,st,et):
    str0="call sh_process.sql_log_info('{}','{}',{},{})".format(pname,sql_str,st,et)
    return str0

def get_time(index):
    time_str="set @time_%d := CURRENT_TIMESTAMP()"%index
    return time_str

for f in files:
    pname=f.split('.')[0]  # 取存储过程名
    fname=os.path.join(fpath,f)
    f_name =os.path.join(wpath,f)
    final_text=[]
    #打开文件 像里面追加信息
    with open(fname,'r',encoding='utf-8') as fp:
        ss=fp.readlines()
        #print(ss)
        str_data=''.join(ss)
        delimiter=str_data.split(';')
        #print(delimiter)
        #先获取索引列表
        index_list=[]

        for j in delimiter:
            if 'call' not in j:
                st=re.search(insert_re,j)
                if st:
                    ind=delimiter.index(j)
                    index_list.append(ind)

                st1 = re.search(update_re, j)
                if st1:
                    ind=delimiter.index(j)
                    index_list.append(ind)


                st2 = re.search(create_re, j)
                if st2:
                    ind=delimiter.index(j)
                    index_list.append(ind)
        #共有几个 update insert create 操作
        num=len(index_list)
        #print(index_list)
        #print(num)
        for i in range(num):
            #print('==',delimiter)
            #每次开始循环之前就重新获取索引值
            index_list1 = []
            for j in delimiter:
                if 'call' not in j:
                    st = re.search(insert_re, j)
                    if st:
                        ind = delimiter.index(j)
                        index_list1.append(ind)

                    st1 = re.search(update_re, j)
                    if st1:
                        ind = delimiter.index(j)
                        index_list1.append(ind)

                    st2 = re.search(create_re, j)
                    if st2:
                        ind = delimiter.index(j)
                        index_list1.append(ind)
            #print(index_list1)
            # 给第一个加上时间
            time_str = get_time(index_list1[i])
            #print(index_list1[i],time_str)
            delimiter.insert(index_list1[i], time_str)
            # 添加结束时间
            time_str = get_time(index_list1[i]+2)
            #print(index_list1[i]+2,time_str)
            delimiter.insert(index_list1[i]+2, time_str)
            # 添加调度日志信息
            #获取对应的sql语句
            #sql_str=delimiter[index_list1[i]+1]
            sql_str='@time_%d--@time_%d'%(index_list1[i],index_list1[i]+2)
            #print('sql:',sql_str)
            log = get_log(sql_str,'@time_%d'%(index_list1[i]),'@time_%d'%(index_list1[i]+2))
            #print(index_list1[i]+3,log)
            delimiter.insert(index_list1[i]+3, log)

        final_str=';\n'.join(delimiter)
        #print(final_str)
        with open(f_name, 'w', encoding='utf-8') as fp:
            fp.write(final_str)









