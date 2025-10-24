# %%
import os
import pandas as pd
import numpy as np
from tqdm import tqdm
from sqlalchemy import create_engine, text
from get_conn import get_conn   # 调用此包会使运行目录变为utils上一级
pd.set_option('future.no_silent_downcasting', True)
# %% 连接数据库
dwbj_conn = get_conn('dwbj')
# print(dwbj_conn)
engine = create_engine(f"postgresql+psycopg2://{dwbj_conn['user']}:{dwbj_conn['password']}@{dwbj_conn['host']}:{dwbj_conn['port']}/{dwbj_conn['database']}")
# %% 定义本工具需要的表（和字段）
subject_dict_name = 'r_subject_dict'
subject_dict_cols = [ 'field_code', 'field_name', 'field_virtual_code', 'field_history_code']
r_check_cross_name = 'r_check_cross'
r_check_cross_cols = ['code', 'accounting_equation', 'condition', 'level', 'tips']
r_check_important_subject_name = 'r_check_important_subject'
r_check_important_subject_cols = ['code', 'subject_code', 'condition', 'level', 'tips']

subject_dict = pd.read_sql(text(f"SELECT {','.join(subject_dict_cols)} FROM {subject_dict_name} "), engine).fillna(np.nan)
r_check_cross = pd.read_sql(text(f"SELECT {','.join(r_check_cross_cols)} FROM {r_check_cross_name}"), engine).fillna(np.nan)
r_check_important_subject = pd.read_sql(text(f"SELECT {','.join(r_check_important_subject_cols)} FROM {r_check_important_subject_name}"), engine).fillna(np.nan).fillna(np.nan)
# %% 定义子函数：拼接字段定义、约束、注释语句、加触发器（固定代码）
def process_check_conditions(r_check_cross: pd.DataFrame, r_check_important_subject:pd.DataFrame, subject_dict:pd.DataFrame):
    """
    删除重复值和hklevel3&替换postgre代码
    """
 
    r_check_cross = r_check_cross.query("~condition.str.contains('hk', na=False)")
    r_check_cross.drop_duplicates(subset=['code', 'condition'], keep='last', inplace=True)
    r_check_cross = r_check_cross.query("level!=3")
 
    r_check_important_subject = r_check_important_subject.query("~subject_code.str.contains('hk', na=False)")
    r_check_important_subject.drop_duplicates(subset=['code', 'subject_code'], keep='last', inplace=True)
    r_check_important_subject = r_check_important_subject.query("level!=3")
    
    # 对科目字典按字段虚拟代码长度排序
    subject_dict_sorted = subject_dict.sort_values(
        by='field_virtual_code', 
        key=lambda x: x.str.len(), 
        ascending=False
    )
    
    # 处理交叉检查的condition转换
    for _, row_conf in r_check_cross.iterrows():
        code = row_conf['code']
        formula = row_conf['condition']
        # 替换公式中的指标code
        for _, row in subject_dict_sorted.iterrows():
            codev = row['field_virtual_code']
            # 获取对应的field_code
            field_code = subject_dict_sorted[
                subject_dict_sorted['field_virtual_code'] == codev
            ]['field_code'].values[0]
            formula = formula.replace(codev, field_code)
        formula = formula.replace('==', '=')
        # 存入转换后的条件
        r_check_cross.loc[r_check_cross['code'] == code, 'condition1'] = formula
    
    # 处理重要科目检查的subject_code转换
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
    
def generate_check_view_sql(r_check_cross:pd.DataFrame, r_check_important_subject:pd.DataFrame,subject_dict:pd.DataFrame):
    """
    生成创建检查视图的SQL语句，包括视图定义和字段注释

    """
    r_check_cross, r_check_important_subject = process_check_conditions(
        r_check_cross=r_check_cross,
        r_check_important_subject=r_check_important_subject,
        subject_dict=subject_dict
    )
    sa = []
    for _, row_conf in r_check_cross.iterrows():
        code = row_conf['code']
        formula = row_conf['condition1']
        ss = f"COALESCE({formula}, false) AS {code}"
        sa.append(ss)

    for _, row_conf in r_check_important_subject.iterrows():
        code = row_conf['code']
        formula = row_conf['condition1']
        ss = f"COALESCE({formula}, false) AS {code}"
        sa.append(ss)
    
    # 生成字段注释语句
    com = []
    for _, row_conf in r_check_cross.iterrows():
        code = row_conf['code']
        comment = row_conf['accounting_equation']
        cc = f"COMMENT ON COLUMN public.c_check.{code} IS '{comment}';"
        com.append(cc)
    
    for _, row_conf in r_check_important_subject.iterrows():
        code = row_conf['code']
        comment = row_conf['tips']
        cc=f"COMMENT ON COLUMN public.c_check.{code} IS '{comment}';"
        com.append(cc)
    
    # 拼接完整SQL语句
    sql = f"""
CREATE OR REPLACE VIEW public.c_check
AS
SELECT 
brf.crmcode AS crmcode,
brf.tyear AS tyear,
brf.tquarter AS tquarter,
{', \n'.join(sa)}
FROM public.b_rpt_finance AS brf
FULL JOIN public.b_rpt_finance_notes AS brfn 
ON brf.crmcode = brfn.crmcode AND brf.tyear = brfn.tyear AND brf.tquarter = brfn.tquarter;
{' \n'.join(com)}
    """
    
    return sql
# %% 测试(自举)
if __name__ == "__main__":
    print(generate_check_view_sql(r_check_cross,r_check_important_subject,subject_dict), end='\n\n')
# %%
