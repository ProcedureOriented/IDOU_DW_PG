# %%
import os
import pandas as pd
import numpy as np
from tqdm import tqdm
from sqlalchemy import create_engine, text, Engine
from typing import Optional, List, Dict, Any, Literal
pd.set_option('future.no_silent_downcasting', True)

# %% 本工具不需要配置表
def insert_pg(df: pd.DataFrame, table_name: str, engine: Engine, schema: str = 'public', 
              batch_size: int = 1000, on_conflict: Optional[List[str]] = None, 
              do: Optional[Literal['nothing','update']] = None, 
              update_set: Optional[List[str]] = None) -> None:
    """通用的PostgreSQL插入/更新函数

    Args:
        df (pd.DataFrame): 待插入/更新的数据.
        table_name (str): 目标表名.
        engine (Engine): 数据库连接引擎.
        schema (str, optional): 目标表所在schema. Defaults to 'public'.
        batch_size (int, optional): 批处理大小. Defaults to 1000.
        on_conflict (Optional[List[str]], optional): 冲突(唯一约束)字段列表. Defaults to None. None表示不处理冲突，直接插入。否则需要同时指定do参数.
        do (Optional[Literal['nothing','update']], optional): 冲突处理策略. Defaults to None. 在on_conflict不为None时必须指定。'nothing'表示冲突时不做任何操作，'update'表示冲突时更新其余字段（或显式指定update_set列表）
        update_set (Optional[List[str]], optional): 更新字段列表. Defaults to None.

    Raises:
        ValueError: 如果DataFrame列名与目标表字段不匹配
        ValueError: 如果do参数不是'nothing'或'update'
    """
    # 访问目标表，获取字段列表
    with engine.connect() as conn:
        result = pd.read_sql(text(f"SELECT column_name FROM information_schema.columns WHERE table_schema = :schema AND table_name = :table_name"), engine, params={'schema': schema, 'table_name': table_name})
        target_cols = result['column_name'].tolist()
    
    # 验证Dataframe列名至少是目标表字段列表的子集,全转化为小写比较
    target_cols = [col.lower() for col in target_cols]
    input_cols = df.columns.str.lower().tolist()
    if not set(input_cols).issubset(set(target_cols)):
        df_cols_only = set(input_cols) - set(target_cols)
        raise ValueError(f"Input has unmatched columns with target table columns: {df_cols_only}")
    
    # 构造SQL语句
    placeholders = ','.join([f":{c}" for c in input_cols])
    sql = f"INSERT INTO {schema}.{table_name} ({', '.join(input_cols)}) VALUES ({placeholders})"
    
    if on_conflict and do:
        # 验证on_conflict字段在目标表中存在
        conflict_cols = [col.lower() for col in on_conflict]
        if not set(conflict_cols).issubset(set(target_cols)):
            conflict_cols_only = set(conflict_cols) - set(target_cols)
            raise ValueError(f"on_conflict has unmatched columns with target table columns: {conflict_cols_only}")
        if do.lower() == 'nothing':
            sql += f" ON CONFLICT ({', '.join(conflict_cols)}) DO NOTHING"
        elif do.lower() == 'update':
            if update_set is None:
                update_set = [col for col in input_cols if col not in on_conflict]
            # 验证update_set字段在目标表中存在
            update_set = [col.lower() for col in update_set]
            if not set(update_set).issubset(set(target_cols)):
                update_set_only = set(update_set) - set(target_cols)
                raise ValueError(f"update_set has unmatched columns with target table columns: {update_set_only}")

            conflict_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_set])
            sql += f" ON CONFLICT ({', '.join(conflict_cols)}) DO UPDATE SET {conflict_set}"
        else:
            raise ValueError("Parameter 'do' must be either 'nothing' or 'update'.")
    
    sql_text = text(sql+";")
    
    # 分批处理Dataframe
    total_rows = len(df)
    batches = total_rows // batch_size + 1
    
    with engine.connect() as conn:
        conn.execute(text("SET TIME ZONE 'Asia/Shanghai'"))
        conn.commit()
        for i in tqdm(range(batches), desc=f"Inserting/Updating {table_name}"):
            start_row = i * batch_size
            end_row = min((i + 1) * batch_size, total_rows)
            if start_row == end_row:
                break
            to_update = df.iloc[start_row:end_row, :].replace(np.nan, None).rename(columns=lambda x: x.lower())
            # sqlalchemy接受将每行作为字典传入的数据形式
            batch_data = to_update.to_dict(orient='records')
            conn.execute(sql_text, batch_data)
        conn.commit()

# %% 测试代码
if __name__ == "__main__":
    from get_conn import get_conn   # 调用此包会使运行目录变为utils上一级

    dwbj_conn = get_conn('dwbj')
    engine = create_engine(f"postgresql+psycopg2://{dwbj_conn['user']}:{dwbj_conn['password']}@{dwbj_conn['host']}:{dwbj_conn['port']}/{dwbj_conn['database']}")

    # raw = pd.read_sql(text("SELECT a.* FROM public.a_finance_wind AS a WHERE crmcode='IB001024' and tyear = 2024"), engine)
    # raw_long = raw.drop(columns=['create_at','update_at']).melt(id_vars=['crmcode','tyear','tquarter'], var_name='subject_code',value_name='subject_value')
    # insert_pg(raw_long, 'a_finance_force', engine, batch_size=300, on_conflict=['crmcode','tyear','tquarter','subject_code'], do='update')