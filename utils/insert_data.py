# %%
import os
import pandas as pd
import numpy as np
from tqdm import tqdm
from sqlalchemy import create_engine, text, Engine
from typing import Optional, List, Dict, Any, Literal
pd.set_option('future.no_silent_downcasting', True)

# %% 本工具不需要配置表
def insert_pgsql(
        df: pd.DataFrame, 
        table_name: str, 
        engine: Engine, 
        schema: str = 'public', 
        batch_size: int = 1000, 
        on_conflict: Optional[List[str]] = None, 
        do: Optional[Literal['nothing','update', None]] = None, 
        update_set: Optional[List[str]] = None,
        extra_stmt: str = ''
    ) -> None:
    """通用的PostgreSQL插入/更新函数

    Args:
        df (pd.DataFrame): 待插入/更新的数据.
        table_name (str): 目标表名.
        engine (Engine): 数据库连接引擎.
        schema (str, optional): 目标表所在schema. Defaults to 'public'.
        batch_size (int, optional): 批处理大小. Defaults to 1000.
        on_conflict (Optional[List[str]], optional): 冲突(唯一约束)字段列表. Defaults to None. None表示不处理冲突，直接插入。否则需要同时指定do参数.
        do (Optional[Literal['nothing','update']], optional): 冲突处理策略. Defaults to None. 在on_conflict不为None时必须指定。'nothing'表示冲突时不做任何操作，'update'表示冲突时更新其余字段（或显式指定update_set列表）
        update_set (Optional[List[str]], optional): 更新字段列表. Defaults to None. 在do为'update'时可选指定。None表示更新除on_conflict字段外的所有字段。
        extra_stmt (str, optional): 在插入语句最后额外SQL语句片段. Defaults to ''.
    """
    # 访问目标表，获取字段列表
    with engine.connect() as conn:
        result = pd.read_sql(text(f"SELECT column_name FROM information_schema.columns WHERE table_schema = :schema AND table_name = :table_name"), engine, params={'schema': schema, 'table_name': table_name})
        target_cols: list[str] = result['column_name'].tolist()
    
    # 验证Dataframe列名至少是目标表字段列表的子集,全转化为小写比较
    target_cols = [col.lower() for col in target_cols]
    input_cols = get_lower_cols(df.columns.tolist())    # 如果DataFrame列名有重复（忽略大小写）会报错
    if not set(input_cols).issubset(set(target_cols)):
        df_cols_only = set(input_cols) - set(target_cols)
        raise ValueError(f"Input has unmatched columns with target table columns: {df_cols_only}")
    
    # 构造SQL语句
    sql = build_core_insert_sql(
        db_type='postgresql',
        table_name=table_name,
        input_cols=input_cols,
        if_duplicate='direct' if on_conflict is None else {'nothing': 'skip', 'update': 'update'}.get(do),  # 未指定on_conflict则直接插入
        pg_conflict=on_conflict,
        if_dup_update=update_set,
        pg_schema=schema,
        extra_stmt=extra_stmt
    )
    sql_text = text(sql)
    
    # 分批处理Dataframe
    total_rows = len(df)
    batches = total_rows // batch_size + 1
    
    with engine.connect() as conn:
        conn.execute(text("SET TIME ZONE 'Asia/Shanghai'"))
        conn.commit()
        for i in tqdm(range(batches), desc=f"[PostgreSQL] Inserting {table_name}"):
            start_row = i * batch_size
            end_row = min((i + 1) * batch_size, total_rows)
            if start_row == end_row:
                break
            to_update = df.iloc[start_row:end_row, :].replace(np.nan, None).rename(columns=lambda x: x.lower())
            # sqlalchemy接受将每行作为字典传入的数据形式
            batch_data = to_update.to_dict(orient='records')
            conn.execute(sql_text, batch_data)
        conn.commit()

def insert_mysql(
        df: pd.DataFrame, 
        table_name: str, 
        engine: Engine, 
        batch_size: int = 1000, 
        if_duplicate_key_action: Optional[Literal['ignore','update', None]] = None, 
        on_duplicate_key_update: Optional[List[str]] = None,
        extra_stmt: str = ''
    ) -> None:
    """通用的MySQL插入/更新函数

    Args:
        df (pd.DataFrame): 待插入/更新的数据.
        table_name (str): 目标表名.
        engine (Engine): 数据库连接引擎.
        batch_size (int, optional): 批处理大小. Defaults to 1000.
        if_duplicate_key_action (Optional[Literal['ignore','update', None]], optional): 冲突处理策略. Defaults to None.
        on_duplicate_key_update (Optional[List[str]], optional): 冲突时更新的字段列表. Defaults to None.
        extra_stmt (str, optional): 在插入语句最后额外SQL语句片段. Defaults to ''. 例如可以为mysql的插入增加`update_time = update_time`来维持原有更新时间戳
    """
    with engine.connect() as conn:
        result = pd.read_sql(text(f"SHOW COLUMNS FROM {table_name}"), engine)
        target_cols: list[str] = result['Field'].tolist()

    # 验证Dataframe列名至少是目标表字段列表的子集,全转化为小写比较
    target_cols = [col.lower() for col in target_cols]
    input_cols = get_lower_cols(df.columns.tolist())    # 如果DataFrame列名有重复（忽略大小写）会报错
    if not set(input_cols).issubset(set(target_cols)):
        df_cols_only = set(input_cols) - set(target_cols)
        raise ValueError(f"Input has unmatched columns with target table columns: {df_cols_only}")

    # 构造SQL语句
    sql = build_core_insert_sql(
        db_type='mysql',
        table_name=table_name,
        input_cols=input_cols,
        if_duplicate='direct' if if_duplicate_key_action is None else {'ignore': 'skip', 'update': 'update'}.get(if_duplicate_key_action),
        if_dup_update=on_duplicate_key_update,
        extra_stmt=extra_stmt
    )
    sql_text = text(sql)

    # 分批处理Dataframe
    total_rows = len(df)
    batches = total_rows // batch_size + 1

    with engine.connect() as conn:
        for i in tqdm(range(batches), desc=f"[MySQL] Inserting {table_name}"):
            start_row = i * batch_size
            end_row = min((i + 1) * batch_size, total_rows)
            if start_row == end_row:
                break
            to_update = df.iloc[start_row:end_row, :].replace(np.nan, None).rename(columns=lambda x: x.lower())
            # sqlalchemy接受将每行作为字典传入的数据形式
            batch_data = to_update.to_dict(orient='records')
            conn.execute(sql_text, batch_data)
        conn.commit()

def build_core_insert_sql(
        db_type: Literal['postgresql','mysql'],
        table_name: str, 
        input_cols: List[str], 
        if_duplicate: Optional[Literal['direct', 'skip', 'update']] = 'direct', 
        pg_conflict: Optional[List[str]] = None,
        if_dup_update: Optional[List[str]] = None,
        pg_schema: str = 'public',
        extra_stmt: str = ''
    ) -> str:
    """构造核心插入SQL语句

    Args:
        db_type (Literal['postgresql','mysql']): 数据库类型
        table_name (str): 表名
        input_cols (List[str]): 输入列名列表
        if_duplicate (Optional[Literal['direct', 'skip', 'update']], optional): 冲突处理策略. Defaults to 'direct'. 默认直接插入（在遇到约束冲突时会报错）。
        pg_conflict (Optional[List[str]], optional): postgresql在if_duplicate为'skip'或'update'时，必须指定唯一约束字段列表(ON CONFLICT (...)). Defaults to None.
        if_dup_update (Optional[List[str]], optional): 在if_duplicate为'update'时，指定需要更新的列名列表，mysql处理重复时必填. Defaults to None，postgre默认处理除pg_conflict以外的所有列，或者显式指定。
        pg_schema (str, optional): PostgreSQL模式名. Defaults to 'public'.
        extra_stmt (str, optional): 在插入语句最后额外SQL语句片段. Defaults to ''. 例如可以为mysql的插入增加`update_time = update_time`来维持原有更新时间戳

    Returns:
        str: 构造的SQL语句
    """
    if db_type not in ['postgresql','mysql']:
        raise ValueError("Unsupported database type. Supported types are 'postgresql' and 'mysql'.")
    if if_duplicate not in ['direct', 'skip', 'update']:
        raise ValueError("Parameter 'if_duplicate' must be either 'direct', 'skip' or 'update'.")
    if db_type == 'postgresql' and if_duplicate in ['skip', 'update'] and pg_conflict is None:
        raise ValueError("PostgreSQL requires unique index columns to handle duplicates (ON CONFLICT (...)).")
    if db_type == 'mysql' and if_duplicate == 'update' and if_dup_update is None:
        raise ValueError("MySQL requires columns to update on duplicates. (ON DUPLICATE KEY UPDATE ...)")
    # 验证input_cols，pg_conflict，if_dup_update各自非空时，转为小写后没有重复列名
    input_cols = get_lower_cols(input_cols)
    pg_conflict = get_lower_cols(pg_conflict) if pg_conflict else pg_conflict
    if_dup_update = get_lower_cols(if_dup_update) if if_dup_update else if_dup_update

    placeholders = ','.join([f":{c}" for c in input_cols])

    match db_type:
        case 'postgresql':
            sql = f"INSERT INTO {pg_schema}.{table_name} ({', '.join(input_cols)}) VALUES ({placeholders})"
            match if_duplicate:
                case 'direct':
                    pass  # 直接插入，无需额外处理
                case 'skip':
                    sql += f" ON CONFLICT ({', '.join(pg_conflict)}) DO NOTHING"
                case 'update':
                    if if_dup_update is None:
                        if_dup_update = [col for col in input_cols if col not in pg_conflict]
                    update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in if_dup_update])
                    sql += f" ON CONFLICT ({', '.join(pg_conflict)}) DO UPDATE SET {update_set}"
                
        case 'mysql':
            sql = ""
            match if_duplicate:
                case 'direct':
                    sql = f"INSERT INTO {table_name} ({', '.join(input_cols)}) VALUES ({placeholders})"
                case 'skip':
                    sql = f"INSERT IGNORE INTO {table_name} ({', '.join(input_cols)}) VALUES ({placeholders})"
                case 'update':
                    sql = f"INSERT INTO {table_name} ({', '.join(input_cols)}) VALUES ({placeholders}) AS new"
                    update_set = ', '.join([f"{col} = new.{col}" for col in if_dup_update])
                    sql += f" ON DUPLICATE KEY UPDATE {update_set}"

        case _:
            raise ValueError("Unsupported database type. Supported types are 'postgresql' and 'mysql'.")

    return sql + extra_stmt +";"

def get_lower_cols(ls: List[str]) -> List[str]:
    """将字符串列表中的所有元素转为小写，有重复时报错

    Args:
        ls (List[str]): 字符串列表

    Returns:
        List[str]: 转为小写后的字符串列表
    """
    lower_ls = [col.lower() for col in ls]
    if len(lower_ls) != len(set(lower_ls)):
        raise ValueError("The input list has duplicate column names when converted to lowercase.")
    return lower_ls

# %% 测试代码
if __name__ == "__main__":
    from get_conn import get_conn   # 调用此包会使运行目录变为utils上一级

    dwbj_conn = get_conn('dwbj')
    engine = create_engine(f"postgresql+psycopg2://{dwbj_conn['user']}:{dwbj_conn['password']}@{dwbj_conn['host']}:{dwbj_conn['port']}/{dwbj_conn['database']}")

    # raw = pd.read_sql(text("SELECT a.* FROM public.a_finance_wind AS a WHERE crmcode='IB001024' and tyear = 2024"), engine)
    # raw_long = raw.drop(columns=['create_at','update_at']).melt(id_vars=['crmcode','tyear','tquarter'], var_name='subject_code',value_name='subject_value')
    # insert_pgsql(raw_long, 'a_finance_force', engine, batch_size=300, on_conflict=['crmcode','tyear','tquarter','subject_code'], do='update')

    test_conn = get_conn('mysql_test')
    engine = create_engine(f"mysql+pymysql://{test_conn['user']}:{test_conn['password']}@{test_conn['host']}:{test_conn['port']}/{test_conn['database']}?charset=utf8mb4")

    # raw_mysql = pd.read_sql(text("SELECT * FROM test_combine_level"), engine)
    # raw_mysql.drop(columns=['id', 'update_time'], inplace=True)
    # raw_mysql['entity_id'] = 'B'
    # raw_mysql['quarter_mark'] = 10
    # insert_mysql(raw_mysql, 'test_combine_level', engine, batch_size=100, if_duplicate_key_action='update', on_duplicate_key_update=[
    #     'year_level',
    #     'year_mark',
    #     'quarter_level',
    #     'quarter_mark',
    #     'behave_level',
    #     'behave_mark',
    #     'combine_level'], extra_stmt=', update_time = update_time')
# %%
