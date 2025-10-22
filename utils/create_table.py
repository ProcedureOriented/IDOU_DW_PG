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
table_info_name = 'r_dict_table_info'
table_info_cols = ['table_code', 'table_name']
field_info_name = 'r_dict_field_info'
field_info_cols = ['table_code', 'field_order', 'field_code', 'field_name', 'data_type_para', 
    'default_value', 'is_not_null', 'enable_status', 'sync_field_code', 'history_code', 'remarks']
table_constraints_name = 'r_dict_table_constraints'
table_constraints_cols = ['owner_table', 'constraint_name', 'constraint_type', 'fk_ref_to',
    'pos01', 'pos02', 'pos03', 'pos04', 'pos05', 'pos06', 'pos07', 'pos08',
    'pos09', 'pos10', 'fk_limit']

table_info = pd.read_sql(text(f"SELECT {','.join(table_info_cols)} FROM {table_info_name} WHERE table_code != '-'"), engine).fillna(np.nan)
field_info = pd.read_sql(text(f"SELECT {','.join(field_info_cols)} FROM {field_info_name}"), engine).fillna(np.nan)
table_constraints = pd.read_sql(text(f"SELECT {','.join(table_constraints_cols)} FROM {table_constraints_name}"), engine).fillna(np.nan).fillna(np.nan)

# %% 定义子函数：拼接字段定义、约束、注释语句、加触发器（固定代码）
def get_field_def(field_info_row: pd.Series, space_indent: int = 4) -> str:
    """拼接字段定义语句"""
    indentation = ' ' * space_indent
    field_code = field_info_row['field_code']
    field_type = ' ' + field_info_row['data_type_para']
    default_value = f" DEFAULT {field_info_row['default_value']}" if pd.notna(field_info_row['default_value']) else ""
    # 当有默认值且允许为空时，nullable为空串，其他情况按定义给出NULL或NOT NULL
    nullable = " NOT NULL" if field_info_row['is_not_null'] else " NULL" # field_info_row['is_not_null']是boolean类型
    nullable = "" if default_value and not field_info_row['is_not_null'] else nullable
    
    field_def = f"{indentation}{field_code}{field_type}{default_value}{nullable}"
    return field_def

def get_constraint_def(constraint_rows: pd.DataFrame, schema: str = 'public', space_indent: int = 4) -> str:
    """拼接约束定义语句，外键的配置有两行，其他约束一行"""
    if constraint_rows.shape[0] == 1:
        const_row = constraint_rows.iloc[0,:]
        const_refrow = pd.Series()
        assert const_row['constraint_type'].lower() != 'fk', "Foreign key constraint should have two rows."
    elif constraint_rows.shape[0] == 2:
        const_row = constraint_rows.query("fk_ref_to == '-'").iloc[0,:]
        const_refrow = constraint_rows.query("fk_ref_to != '-'").iloc[0,:]
        # 只检查外键约束配对的主行type
        assert const_row['constraint_type'].lower() == 'fk', "One of the two constraint rows should be foreign key."
    else:
        raise ValueError("Constraint rows should be one or two rows only.")
    
    indentation = ' ' * space_indent
    owner_table_code = const_row['owner_table']  # 正常情况不会拼接用到
    const_name = const_row['constraint_name']
    const_type: str  = const_row['constraint_type']
    const_cols: list = const_row.filter(like='pos').dropna().tolist()
    const_cols: str  = ', '.join(const_cols)

    match const_type.lower():
        case 'pk':
            const_def = f"{indentation}CONSTRAINT {const_name} PRIMARY KEY ({const_cols})"
        case 'uq':
            const_def = f"{indentation}CONSTRAINT {const_name} UNIQUE ({const_cols})"
        case 'idx':
            const_def = f"{indentation}CONSTRAINT {const_name} INDEX ({const_cols})"
        case 'fk':
            ref_table_code = const_refrow['fk_ref_to']
            ref_table_cols: list = const_refrow.filter(like='pos').dropna().tolist()
            ref_table_cols: str  = ', '.join(ref_table_cols)
            ref_limit = const_refrow['fk_limit']
            assert pd.notna(ref_limit), "Foreign key constraint must have fk_limit defined, like 'ON DELETE RESTRICT ON UPDATE CASCADE'."
            const_def = f"{indentation}CONSTRAINT {const_name} FOREIGN KEY ({const_cols}) REFERENCES {schema}.{ref_table_code}({ref_table_cols}) {ref_limit}"
        case _:
            raise ValueError(f"{owner_table_code}: Unsupported constraint type: {const_type}")

    return const_def

def get_field_comment(field_info_row: pd.Series, schema: str = 'public') -> str:
    table_code = field_info_row['table_code']
    field_code = field_info_row['field_code']
    field_name = field_info_row['field_name'] if pd.notna(field_info_row['field_name']) else ''
    history_code1 = ', ' + field_info_row['sync_field_code'] if pd.notna(field_info_row['sync_field_code']) else ''
    history_code2 = ', ' + field_info_row['history_code'] if pd.notna(field_info_row['history_code']) else ''
    extra_remarks = ': ' + field_info_row['remarks'] if pd.notna(field_info_row['remarks']) else ''

    comment_def = f"COMMENT ON COLUMN {schema}.{table_code}.{field_code} IS '{field_name}{history_code1}{history_code2}{extra_remarks}';"
    return comment_def

def get_table_comment(table_info_row: pd.Series, schema: str = 'public') -> str:
    if pd.isna(table_info_row['table_name']):
        return ""
    else:
        table_code = table_info_row['table_code']
        table_name = table_info_row['table_name']
        comment_def = f"COMMENT ON TABLE {schema}.{table_code} IS '{table_name}';"
        return comment_def

def get_trigger_def(table_code: str, suffix: str = 'update', function: str = 'set_update_at', schema: str = 'public') -> str:
    trigger_def = f"""CREATE TRIGGER {table_code}_{suffix} BEFORE 
    UPDATE ON {schema}.{table_code} 
    FOR EACH ROW EXECUTE FUNCTION {function}();"""
    return trigger_def

# %%
def create_table_sql(table_code: str, schema: str = 'public') -> str:
    """生成建表SQL语句"""
    global table_info, field_info, table_constraints
    # 取出表信息
    table_info_row: pd.Series = table_info.query("table_code == @table_code").iloc[0,:]
    field_info_rows: pd.DataFrame = field_info.query("table_code == @table_code").sort_values(by='field_order')
    constraint_rows: pd.DataFrame = table_constraints.query("owner_table == @table_code").sort_values(by=['constraint_name', 'fk_ref_to'])

    # 拼接字段定义语句
    field_defs = []
    for _, field_row in field_info_rows.iterrows():
        field_def = get_field_def(field_row, space_indent=4)
        field_defs.append(field_def)
    
    # 拼接约束定义语句
    constraint_defs = []
    if not constraint_rows.empty:
        # 按constraint_name分组处理，每组一条或两条
        for const_name, const_group in constraint_rows.groupby('constraint_name'):
            constraint_def = get_constraint_def(const_group, schema=schema, space_indent=4)
            constraint_defs.append(constraint_def)
    
    # 组合建表语句
    all_defs_str = ',\n'.join(field_defs + constraint_defs)
    create_table_stmt = f"CREATE TABLE IF NOT EXISTS {schema}.{table_code} (\n{all_defs_str}\n);\n"

    # 拼接注释语句
    table_comment_stmt = get_table_comment(table_info_row, schema=schema)

    field_comment_stmts = []
    for _, field_row in field_info_rows.iterrows():
        field_comment_stmt = get_field_comment(field_row, schema=schema)
        field_comment_stmts.append(field_comment_stmt)
    field_comment_stmts = '\n'.join(field_comment_stmts)

    # 拼接触发器语句
    # 如果字段清单存在update_at字段，则添加触发器
    if 'update_at' in field_info_rows['field_code'].values:
        trigger_stmt = get_trigger_def(table_code, suffix='update', function='set_update_at', schema=schema)
    else:
        trigger_stmt = ''

    # 组合所有语句
    extra_stmts = '\n\n'.join([stmt for stmt in [table_comment_stmt, field_comment_stmts, trigger_stmt] if stmt])
    full_sql = create_table_stmt + extra_stmts

    return full_sql

# %% 测试(自举)
if __name__ == "__main__":
    for t in ['r_dict_table_info', 'r_dict_field_info', 'r_dict_field_sources', 'r_dict_table_constraints']:
        print(create_table_sql(t), end='\n\n')