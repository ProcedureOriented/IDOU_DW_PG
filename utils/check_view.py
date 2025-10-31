# %%
import os
import pandas as pd
import numpy as np
from tqdm import tqdm
from sqlalchemy import create_engine, text
from create_table import get_field_comment
from get_conn import get_conn   # 调用此包会使运行目录变为utils上一级
pd.set_option('future.no_silent_downcasting', True)
from utils.formula import *
# %% 连接数据库
dwbj_conn = get_conn('dwbj')
# print(dwbj_conn)
engine = create_engine(f"postgresql+psycopg2://{dwbj_conn['user']}:{dwbj_conn['password']}@{dwbj_conn['host']}:{dwbj_conn['port']}/{dwbj_conn['database']}")
# %% 定义本工具需要的表（和字段）
subject_dict_name = 'r_subject_dict'
subject_dict_cols = [ 'field_code', 'field_name', 'field_virtual_code', 'field_history_code']
r_check_cross_name = 'r_check_cross'
r_check_cross_cols = ['code', 'accounting_equation', 'condition', 'level', 'tips','model_code','keyword_code']
r_check_important_subject_name = 'r_check_important_subject'
r_check_important_subject_cols = ['code', 'subject_code', 'condition', 'level', 'tips','model_code','keyword_code']
table_info_name = 'r_dict_table_info'
table_info_cols = ['table_code', 'table_name']
field_info_name = 'r_dict_field_info'
field_info_cols = ['table_code', 'field_order', 'field_code', 'field_name', 'data_type_para', 
    'default_value', 'is_not_null', 'enable_status', 'sync_field_code', 'history_code', 'remarks']
table_info = pd.read_sql(text(f"SELECT {','.join(table_info_cols)} FROM {table_info_name} WHERE table_code != '-'"), engine).fillna(np.nan)
field_info = pd.read_sql(text(f"SELECT {','.join(field_info_cols)} FROM {field_info_name}"), engine).fillna(np.nan)
subject_dict = pd.read_sql(text(f"SELECT {','.join(subject_dict_cols)} FROM {subject_dict_name} "), engine).fillna(np.nan)
r_check_cross = pd.read_sql(text(f"SELECT {','.join(r_check_cross_cols)} FROM {r_check_cross_name}"), engine).fillna(np.nan)
r_check_important_subject = pd.read_sql(text(f"SELECT {','.join(r_check_important_subject_cols)} FROM {r_check_important_subject_name}"), engine).fillna(np.nan).fillna(np.nan)
# %% 定义子函数：拼接字段定义、约束、注释语句、加触发器（固定代码）
def process_check_conditions(r_check_cross: pd.DataFrame, r_check_important_subject:pd.DataFrame, subject_dict:pd.DataFrame):
    """
    删除重复值和hklevel3&替换postgre代码
    """
 
    r_check_cross = r_check_cross.query("model_code=='model1'")
    r_check_cross = r_check_cross.query("level==1 |level==2")
 
    r_check_important_subject = r_check_important_subject.query("model_code=='model1'")
    r_check_important_subject = r_check_important_subject.query("level==1|level==2")
    
    # 对科目字典按字段虚拟代码长度排序
    subject_dict_sorted = subject_dict.sort_values(
        by='field_virtual_code', 
        key=lambda x: x.str.len(), 
        ascending=False
    )
    
    #将公式中除了keyword套COALESCE&替换公式中的code和==条件
    for _, row_conf in r_check_cross.iterrows():
        code = row_conf['code']
    # 第一次处理：替换非关键字为COALESCE并处理==
        formula = row_conf['condition']
        keyword = row_conf['keyword_code']
        otherword = [i for i in parse_fields(formula) if str(i) != str(keyword)]
        for i in otherword:
            ci = f"COALESCE({i}, 0)"
            formula = formula.replace(i, ci)
        formula = formula.replace('==', '=')
    # 第二次处理：替换指标code为field_code
        for _, row in subject_dict_sorted.iterrows():
            codev = row['field_virtual_code']
            field_code = subject_dict_sorted[
                subject_dict_sorted['field_virtual_code'] == codev
            ]['field_code'].values[0]
            formula = formula.replace(codev, field_code)
    # 最终赋值到condition1
        r_check_cross.loc[r_check_cross['code'] == code, 'condition1'] = formula


    for _, row_conf in r_check_important_subject.iterrows():
        code = row_conf['code']
        formulao = row_conf['subject_code']
    # 替换公式中的指标code
        for _, row in subject_dict_sorted.iterrows():
            codev = row['field_virtual_code']
        # 获取对应的field_code并拼接条件
            field_code = subject_dict_sorted[
                subject_dict_sorted['field_virtual_code'] == codev
            ]['field_code'].values[0]
            formulao = formulao.replace(codev, f"{field_code}<>0")
    # 存入转换后的条件
        r_check_important_subject.loc[
            r_check_important_subject['code'] == code, 
            'condition1'
        ] = formulao
    return r_check_cross,r_check_important_subject
    


def generate_check_view_sql(r_check_cross:pd.DataFrame, r_check_important_subject:pd.DataFrame,subject_dict:pd.DataFrame,table_code: str, schema: str = 'public'):
    """
    生成创建检查视图的SQL语句，包括视图定义和字段注释

    """
    field_info_rows: pd.DataFrame = field_info.query("table_code == @table_code").sort_values(by='field_order')
    
    r_check_cross, r_check_important_subject = process_check_conditions(
        r_check_cross=r_check_cross,
        r_check_important_subject=r_check_important_subject,
        subject_dict=subject_dict
    )
    sa=[]
    for _, row_conf in r_check_cross.iterrows():
        code = row_conf['code']
        formula = row_conf['condition1']
        level=row_conf['level']
        ss=f"""case  
        when {formula} THEN 0
        ELSE {level}
        END AS {code}"""
        sa.append(ss)
    for _, row_conf in r_check_important_subject.iterrows():
        code = row_conf['code']
        formula = row_conf['condition1']
        level=row_conf['level']
        ss=f"""case  
        when {formula} THEN 0
        ELSE {level}
        END AS {code}"""
        sa.append(ss)

    # 生成字段注释语句
    field_comment_stmts = []
    for _, field_row in field_info_rows.iterrows():
        field_comment_stmt = get_field_comment(field_row, schema=schema)
        field_comment_stmts.append(field_comment_stmt)
    field_comment_stmts = '\n'.join(field_comment_stmts)

    # 拼接完整SQL语句
    sql = f"""
CREATE OR REPLACE VIEW public.c_check
AS
SELECT 
tad2.crmcode AS crmcode,
tad2.tyear AS tyear,
tad2.tquarter AS tquarter,
{', \n'.join(sa)}
FROM public.temp_avaliable_data2 AS tad2;
{(field_comment_stmts)}
    """
    
    return sql
# %% 测试(自举)
if __name__ == "__main__":
    print(generate_check_view_sql(r_check_cross,r_check_important_subject,subject_dict,'c_check'), end='\n\n')
# %%
