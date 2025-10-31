import pandas as pd
import numpy as np
import re
import warnings
from typing import List, Tuple, Dict, Set, Optional, Union, Any, Mapping, Literal
from .custom_typing import normal_fdict, timecalc_fdict, stats_fdict, paramjudge_fdict

_formula_comparision_operators = [
    '==', '!=', '<=', '>=', '<', '>', 
    '.isna()', '.isnull()', '.notna()', '.notnull()', 
    '.isin', '.str.contains',
]

_formula_special_heads = {
    '统计': ['count', 'sum', 'mean', 'quantile'],
    '时间计算': ['year', 'Y', 'quarter', 'Q', 'month', 'M', 'day', 'D'],
}

_formula_head_mapper = {
    head: calc_type for calc_type, head_group in _formula_special_heads.items() 
    for head in head_group
}

_formula_special_words = (
    'match', 'exact_match',
    'year', 'Y',
    'quarter', 'Q',
    'month', 'M',
    'day', 'D',
    'count', 'sum', 'mean', 'quantile',
    'abs', 
    'True', 'False', 'TRUE', 'FALSE', 'true', 'false',
)

_formula_unify_mapper = {
    '～': '~',
    '（': '(',
    '）': ')',
    '＋': '+',
    '－': '-',
    '×': '*',
    '÷': '/',
    '，': ',',
    '：': ':',
    '；': ';',
    '＝': '=',
    '＜': '<',
    '＞': '>',
    '≤': '<=',
    '≥': '>=',
    '≠': '!=',
    '％': '%',
    '＃': '#',
    '＆': '&',
    '＠': '@',
    '＄': '$',
    '＊': '*',
    '＂': '"',
    '“': '"',
    '”': '"',
    '＇': "'",
    '‘': "'",
    '’': "'",
    '［': '[',
    '］': ']',
    '｛': '{',
    '｝': '}',
    '｜': '|',
    '　': ' ',
    '／': '/',
}

_field_shift_operation_mapper = {
    '^': 'Y',   # 年份偏移，1为取上一期，-1为取未来一期
    '~': 'Q',   # 季度偏移，1为取上一个季度，-1为取未来一个季度
    '°': 'YEND',# 年末偏移，1为取上一年年末，-1为取未来一年年末，0为本年末
}
_field_shift_operators = [re.escape(x) for x in _field_shift_operation_mapper.keys()]
_field_shift_pattern = re.compile('(.*)(' + '|'.join(_field_shift_operators) + ')(.*)')
_field_executable_pattern = re.compile(r'([a-zA-Z0-9_]+)\.[a-z]+')

def simple_cleaning(text: Union[str, pd.Series]) -> Union[str, pd.Series]:
    """
    简单清洗器：去除空格，中文逗号转换英文逗号，通常用于参数的清洗。\n
    元素层面：series中的nan会被填充为''，'-'会被替换为''
    """
    if isinstance(text, pd.Series):
        text = text.fillna('')
        text = text.str.replace('-', '')
        text = text.str.replace(' ', '')
        text = text.str.replace('，', ',')
    elif isinstance(text, str):
        text = text.replace(' ', '')
        text = text.replace('，', ',')
        text = '' if text in ['-', 'nan', 'NaN', 'NAN', 'None'] else text
    else:
        print('简单清洗器：输入类型错误，无处理')

    return text

def clean_formula(formula: Union[str, pd.Series]) -> Any:
    '''
    公式清洗器：接收pd.Series或str, 返回对应类型。仅处理特殊单字符和空格
    '''
    if isinstance(formula, pd.Series):
        nan_bidx = formula.isna()
        for k, v in _formula_unify_mapper.items():
            formula = formula.astype(str).str.replace(k, v)
        formula = formula.str.replace(' ', '')  # 处理空格
        formula[nan_bidx] = np.nan
    
    elif isinstance(formula, str):
        mapper_keyset = set(_formula_unify_mapper.keys())
        targets = mapper_keyset.intersection(formula)
        for k in targets:
            formula = formula.replace(k, _formula_unify_mapper[k])
        formula = formula.replace(' ', '')  # 处理空格
    
    else:
        print('公式清洗器：输入类型错误，无处理')

    return formula
    
def parse_formula(formula: Union[str, pd.Series]) -> Union[dict, pd.Series]:
    '''
    公式拆解器：接收pd.Series或str，按“;”拆解公式，根据特别的特征识别公式类型，返回公式字典
    '''
    if isinstance(formula, pd.Series):
        formula = formula.apply(_parse_formula)
    elif isinstance(formula, str):
        formula = _parse_formula(formula)
    else:
        print('公式拆解器：输入类型错误，无处理')

    return formula

def _parse_formula(formula: str) -> dict:
    '''
    公式拆解器：按“;”拆解公式，根据特别的特征识别公式类型，返回公式字典，如果未识别到类型作为不定参判断
    '''
    # formula拆解原则：不论是单一可执行公式还是带参数公式，最后返回的formula['formula']应该包含公式用到的全部信息
    # 以便通过parse_fields获取完整的计算表
    if ';' not in formula:
        # 计算、定参判断类型使用单一公式
        assert ',' not in formula, f'公式拆解器：单一计算公式中存在“,”，请检查: \n{formula}\n'
        # 如果包含任何判断符号，认为是定参判断公式
        if any([sign in formula for sign in _formula_comparision_operators]):
            f_type = '定参判断'
            verified = verify_pythonic_formula(formula)
            assert verified, f'公式拆解器：单一定参判断公式中存在不规范的单等号或<>，请检查: \n{formula}\n'
        else:
            f_type = '计算'
        f_dict: normal_fdict = {'_type': f_type, 'formula': formula}

    else:
        # 时间计算、统计、不定参判断使用分号逗号参数型公式
        # 拆解成双层列表
        f_list = formula.split(';')
        f_list = [substr.split(',') for substr in f_list]
        f_head = f_list[0][0]
        f_type = _formula_head_mapper.get(f_head, '不定参判断')

        if f_type == '时间计算': 
            f_dict: timecalc_fdict = {
                '_type': f_type,
                'time_unit': f_head,
            }
            f_dict['formula'] = f_list[1][0]

        elif f_type == '统计':
            # 字典中，如果不存在f_list[0][1]，'stat_field'赋值为''
            f_dict: stats_fdict = {
                '_type': f_type,
                'stats_method': f_head,
                'stats_field' : ','.join(f_list[0][1:]) if len(f_list[0]) > 1 else '',
                'group_keys' : f_list[1],
                'condition': f_list[2][0],
                'add_condition': f_list[2][0],  # 旧名称
            }
            f_dict['formula'] = ','.join([f_dict['stats_field'], *f_dict['group_keys'], f_dict['condition']])

        elif f_type == '不定参判断':
            # 不定参的第一个参数为判断字段，第二组参数为判断用到的字段
            f_dict: paramjudge_fdict = {
                '_type': f_type,
                'target': f_head,
                'direction': f_list[0][1],
                'dimension_names': f_list[1],
            }
            f_dict['formula'] = f_head + '+' + '*'.join(f_list[1])

    return f_dict

def verify_pythonic_formula(formula: str) -> bool:
    '''
    验证公式使用的比较符是否符合Python语法：双等于和不等于的形式；\n
    存在单个等号或<>，代表该公式不符合Python语法。存在对参数赋值形式的字符串不应使用此函数。
    '''
    for sign in _formula_comparision_operators:
        formula = formula.replace(sign, '')
    return '=' not in formula and '<>' not in formula

def parse_fields(formula: Union[str, pd.Series], unique: bool=True) -> Any:
    '''
    字段解析器：传入字符串公式，返回公式中使用的字段，由unique指定是否去重和去后缀
    '''
    formula = clean_formula(formula)
    formula = parse_formula(formula)   # 字典或一列储存字典的series, str->dict, pd.Series->pd.Series

    if isinstance(formula, pd.Series):
        formula_df = pd.DataFrame(formula.tolist())
        formula = formula_df['formula'].apply(_parse_fields, unique=unique)
    elif isinstance(formula, dict):
        formula = _parse_fields(formula['formula'], unique=unique)
    else:
        print('字段解析器：输入类型错误，无处理')

    return formula
    
def _parse_fields(formula: str, unique: bool=True) -> list:
    '''
    字段解析器：传入字符串公式，返回公式中使用的字段，由unique指定是否去重和去后缀
    '''
    # 要分列的运算符
    stop_symbol = list(r',+-*/()=<>%#&|')
    # 保护性例外
    exception_mapper = {
        '^-': '^^',
        '~-': '~~',
        '°-': '°°',
    }
    # 替换例外
    for k, v in exception_mapper.items():
        formula = formula.replace(k, v)
    # 替换停用符号
    for s in stop_symbol:
        formula = formula.replace(s, ' ')
    # 去除连续空格
    while '  ' in formula:
        formula = formula.replace('  ', ' ')
    # 去除首尾空格
    formula = formula.strip()
    # 还原例外，再按空格拆分
    for k, v in exception_mapper.items():
        formula = formula.replace(v, k)
    fields: list[str] = formula.split(' ')

    # 对fields中每个字符串进行处理:
    # 如果是纯数字、小数或包含英文双引号"，删除;
    # 如果是特殊词，删除;
    # 如果包含.，在末尾加上()
    for i, f in enumerate(fields):
        condition = [
            f.replace('.', '').replace('-', '').isdigit(),
            '"' in f,
            f in _formula_special_words,
        ]
        if any(condition):
            fields[i] = ''
            f = fields[i]
        if '.' in f and not f.endswith(')'):
            fields[i] = f + '()'
    # 去除空字符串
    fields = [f for f in fields if f != '']

    # 如果unique为True，去重和去后缀，后缀为^或~或°或.及其之后的所有字符
    if unique:
        # 保留后缀标记前的pattern
        pattern = re.compile(r'[^~^°.]+')
        fields = list(set(fields))
        fields = [pattern.match(f).group() for f in fields]
        fields = list(set(fields))

    return fields

def translate_special_field(field: Union[str, List[str]], sign: str = None, error: Literal['ignore', 'warn', 'raise'] = 'warn') -> Union[str, dict]:
    """
    将特殊指标格式转换为简单形式，与指标特殊处理后的列名相同。\n
    只有时间偏移标记会被转换，无特殊标记和其他特殊标记（可执行）会返回原始字段。

    Parameters
    ----------
        field: 被特殊标记的字段，可以是原始字段，此时需要sign指定特殊标记部分
        sign: 特殊标记. Defaults to None.
        error: 无法识别的特殊字段处理方式. Defaults to 'warn'.
    """
    format_str = "{origin}_{unit}{direction}{abs_offset}"
    if isinstance(field, list):
        translate_dict = {f: translate_special_field(f, sign, error) for f in field}
        # 按key的长度从长到短排序
        translate_dict = dict(sorted(translate_dict.items(), key=lambda x: len(x[0]), reverse=True))
        return translate_dict
        
    elif isinstance(field, str):
        parsed = parse_special_field(field, sign, error)
        if parsed['_success'] and 'unit' in parsed:
            return format_str.format(**parsed, abs_offset=abs(parsed['offset']))
        else:
            return field
    
    else:
        raise ValueError(f"translate_special_field: 未知的字段类型 {field}")
        
def parse_special_field(field: str, sign: str = None, error: Literal['ignore', 'warn', 'raise'] = 'warn') -> dict:
    """
    将特殊指标拆解成字典：原指标、操作、参数

    Parameters
    ----------
        field: 被特殊标记的字段，可能是时间偏移标记或其他pandas可执行后缀。可以是原始字段，此时需要sign指定时间偏移标记部分
        sign: 时间偏移的特殊标记. Defaults to None.

    Returns
    -------
        - origin: 原始字段
        - unit: 操作单位
        - direction: 操作方向
        - offset: 操作偏移量，方向为backward时，取历史时期，偏移量为正数；
            方向为forward时，取未来时期，偏移量为负数；
            方向为current时，取当前时期（一般为取当年末期会出现此情况），偏移量为0
    """
    if _field_executable_pattern.match(field):
        origin = _field_executable_pattern.match(field).group(1)
        executable_suffix = field.replace(origin, '', 1)
        return {'origin': origin, 'executable': executable_suffix, '_success': True}

    if sign is None:
        try:
            match = _field_shift_pattern.match(field)
            field = match.group(1)
            operator = match.group(2)
            param = match.group(3)
            sign = operator+param
        except:
            assert error in ['ignore', 'warn', 'raise'], f"parse_special_field: 未定义的错误处理方式 {error}"
            if error == 'raise':
                raise ValueError(f"parse_special_field: 未定义的特殊字段 {field}")
            elif error == 'warn':
                warnings.warn(f"parse_special_field: 未定义的特殊字段 {field}", UserWarning)
                return {'origin': field, 'unit': '', 'direction': '', 'offset': '', '_success': False}
            else:
                return {'origin': field, 'unit': '', 'direction': '', 'offset': '', '_success': False}

    sign = sign.lower()
    if sign in ['本期', '当期', '']:
        return {'origin': field, 'unit': '', 'direction': '', 'offset': '', '_success': False}
    
    if 'end' in sign or '末' in sign or '°' in sign:
        # 默认年末季度偏移
        operating_unit = 'YEND'
    elif 'year' in sign or '年' in sign or '^' in sign or '期' in sign:
        operating_unit = 'Y'
    elif 'quarter' in sign or '季' in sign or '~' in sign:
        operating_unit = 'Q'
    elif 'month' in sign or '月' in sign:
        operating_unit = 'M'
    else:
        raise ValueError(f'parse_special_fields：未识别时间偏移标记 {sign}')

    if '本' in sign or '当' in sign or '°0' in sign:
        direction = 'current'
    elif '下' in sign or '^-' in sign or '~-' in sign or '°-' in sign:
        direction = 'forward'
    elif '上' in sign or '^' in sign or '~' in sign or '°' in sign:
        direction = 'backward'
    else:
        raise ValueError(f'parse_special_fields：未识别时间偏移方向 {sign}')

    try:
        offset_num = int(re.search(r'[\-]?\d+', sign).group())
        if direction == 'forward':
            offset_num = -abs(offset_num)
    except:
        if '上' in sign:
            offset_num = sign.count('上')
        elif '下' in sign:
            offset_num = -sign.count('下')
        elif '本' in sign or '当' in sign or '°0' in sign:
            offset_num = 0
        else:
            raise ValueError(f'parse_special_fields: 未找到时间偏移量 {sign}')
        
    return {
        'origin': field,
        'unit': operating_unit,
        'direction': direction,
        'offset': offset_num,
        '_success': True,
    }

def range2params(range: str) -> List[Tuple[float, float, str]]:
    '''
    将多个值域的字符串拆分，类似"(-2,-1],[1,2)"，返回一个列表，每个元素为一个值域的元组
    '''
    range: str = clean_formula(range)
    range.replace('),', ')|')
    range.replace('],', ']|')
    multi_range = range.split('|')
    multi_range = [range2tuple(r) for r in multi_range]

    return multi_range

def range2tuple(range: str, numeric: bool = False) -> Tuple[float, float, str]:
    '''
    将类似于值域的字符串拆分，返回左边界、右边界和指示开闭区间的字符串，适用于pandas的between()中的inclusive参数
    '''

    range = clean_formula(range)
    range_list = range.split(',')
    assert len(range_list) == 2, f'值域格式错误：{range}'
    # 取出第一个元素的第一个字符之后的字符串为左边界，
    # 取出第二个元素的最后一个字符之前的字符串为右边界，
    # 取出第一个元素的第一个字符和第二个元素的最后一个字符，拼接为开闭区间字符串
    range_list = [
        range_list[0][1:],
        range_list[1][:-1],
        range_list[0][0] + range_list[1][-1],
    ]
    inclusive_dict = {
        '[]': 'both',
        '[)': 'left',
        '(]': 'right',
        '()': 'neither',
    }
    # 将左右区间转换为数字类型
    range_list[0] = unify_number(range_list[0]) if numeric else range_list[0]
    range_list[1] = unify_number(range_list[1]) if numeric else range_list[1]
    range_list[2] = inclusive_dict[range_list[2]]

    return range_list
    
def unify_number(string: str) -> Union[int, float]:
    '''
    将字符串形式的字符串转换为数字；
    inf和∞将被作为'np.inf'处理
    '''
    # 去除string中可能存在的-
    is_negative = '-' if string[0] == '-' else ''
    string = string.replace('-', '')
    # 完整化inf字符串
    if 'inf' in string.lower():
        string = 'np.inf'
    if '∞' in string:
        string = 'np.inf'
    string = is_negative + string

    try:
        number = eval(string)
    except:
        raise ValueError(f'无法将{string}转换为数字')
    
    return number

def config2range(method: str, thresholds: List[Union[int, float]]) -> str:
    '''
    将双列参数转换为值域字符串, 双阈值默认为圆括号（不包括边界）
    “大于”、“0.1” -> (0.1, +∞)
    “外右包含”、“0.1,0.2” -> (-∞, 0.1)|[0.2, +∞)
    '''
    assert len(thresholds) in [1, 2], f"gen_compare_range: 阈值数量错误 {thresholds}"

    if len(thresholds) == 1:
        th1 = thresholds[0]
        if method in ['大于', '>']:
            return f"({th1}, +∞)"
        elif method in ['小于', '<']:
            return f"(-∞, {th1})"
        elif method in ['大于等于', '>=']:
            return f"[{th1}, +∞)"
        elif method in ['小于等于', '<=']:
            return f"(-∞, {th1}]"
        else:
            raise ValueError(f"gen_compare_range: 未知的单参数比较方法 {method}")
    else:
        thresholds.sort()
        th1, th2 = *thresholds,

        if '外' in method:
            left_bound  = ']' if '左包' in method else ')' # 左包含，左包括……
            right_bound = '[' if '右包' in method else '(' # 右包含，右包括……
            return f"(-∞, {th1}{left_bound}|{right_bound}{th2}, +∞)"
        elif '内' in method:
            left_bound  = '[' if '左包' in method else '(' # 左包含，左包括……
            right_bound = ']' if '右包' in method else ')' # 右包含，右包括……
            return f"{left_bound}{th1}, {th2}{right_bound}"
        else:
            raise ValueError(f"gen_compare_range: 未知的双参数比较方法 {method}")

def gen_compare_formula(code: str, method: str, thresholds: List[Union[str, int, float]]) -> str:
    '''
    将双列参数转换为pandas.eval()可用的比较语句
    “大于”、“0.1” -> code>0.1
    也可传入列名用于整列比较
    '''
    if isinstance(thresholds, str) or isinstance(thresholds, int) or isinstance(thresholds, float):
        thresholds = [thresholds]
    
    assert len(thresholds) in [1, 2], f"gen_compare_formula: 阈值数量错误 {thresholds}"
    if any([
        '大于' in method,
        '小于' in method,
        method in ['>', '<', '>=', '<='],
        pd.isna(thresholds[1]),
        bool(thresholds[1]) == False,
    ]):
        thresholds = [thresholds[0]]

    if len(thresholds) == 1:
        th1 = thresholds[0]
        if method in ['大于', '>']:
            return f"{code}>{th1}"
        elif method in ['小于', '<']:
            return f"{code}<{th1}"
        elif method in ['大于等于', '>=']:
            return f"{code}>={th1}"
        elif method in ['小于等于', '<=']:
            return f"{code}<={th1}"
        else:
            raise ValueError(f"gen_compare_formula: 未知的单参数比较方法 {method}")
    else:
        if not isinstance(thresholds[0], str):
            thresholds.sort()
        th1, th2 = *thresholds,

        left_eq  = '=' if '左包' in method else '' # 左包含，左包括……
        right_eq = '=' if '右包' in method else '' # 右包含，右包括……
            
        if '外' in method:
            return f"{code}<{left_eq}{th1} | {code}>{right_eq}{th2}"
        elif '内' in method:
            return f"{code}>{left_eq}{th1} & {code}<{right_eq}{th2}"
        else:
            raise ValueError(f"gen_compare_formula: 未知的双参数比较方法 {method}")
        
def gen_concat_formula(groups: List[list], operator: str) -> str:
    """
    按编组生成条件公式的组合

    Parameters
    ----------
        groups: 编组为双层列表，内层为每个语句的元素，外层为每个编组
        operator: 链接每个语句的逻辑关系符

    Returns
    -------
        条件字符串用于pandas.eval()，无条件返回np.nan
    """
    valid_groups = [g for g in groups if {'', '-', 'nan', 'NaN', 'NAN', 'None'}.intersection(g) == set() and pd.Series(g).isnull().sum() == 0]
    if len(valid_groups) == 0:
        return np.nan
    
    else:
        valid_formulas = [''.join(g) for g in valid_groups]
        if len(valid_formulas) == 1:
            formula = valid_formulas[0]
        else:
            formula = '(' + f"){operator}(".join(valid_formulas) + ')'

        return formula

# 未使用的函数
def get_timeoffset_map(source: Union[int, pd.Series, pd.DataFrame], sign: str, unit_col_info: Mapping = None) -> Union[dict, pd.Series, pd.DataFrame]:
    '''
    时间偏移映射器：
    '''
    sign = sign.lower()
    if 'end' in sign or '末' in sign or '°' in sign:
        # 默认年末季度偏移
        operating_unit = 'YEND'
        assert 'year_col' in unit_col_info, '时间偏移映射器：未指定年份列'
        assert 'quarter_col' in unit_col_info, '时间偏移映射器：年末季度偏移未指定季度列'

    elif 'year' in sign or '年' in sign or '^' in sign or '期' in sign or isinstance(source, int):
        operating_unit = 'Y'
        if isinstance(source, pd.DataFrame):
            assert 'year_col' in unit_col_info, '时间偏移映射器：未指定年份列'

    elif 'quarter' in sign or '季' in sign or '~' in sign:
        operating_unit = 'Q'
        assert 'year_col'    in unit_col_info, '时间偏移映射器：未指定年份列'
        assert 'quarter_col' in unit_col_info, '时间偏移映射器：未指定季度列'

    elif 'month' in sign or '月' in sign:
        operating_unit = 'M'
        assert 'year_col'  in unit_col_info, '时间偏移映射器：未指定年份列'
        assert 'month_col' in unit_col_info, '时间偏移映射器：未指定月份列'
    
    else:
        raise ValueError(f'时间偏移映射器：未识别时间偏移单位 {sign}')

    if '本' in sign or '当' in sign or '°0' in sign:
        direction = 'current'
    elif '下' in sign or '^-' in sign or '~-' in sign:
        direction = 'forward'
    elif '上' in sign or '^' in sign or '~' in sign:
        direction = 'backward'

    try:
        offset_num = int(re.search(r'[\-]?\d+', sign).group())
        if direction == 'forward':
            offset_num = -abs(offset_num)
    except:
        if '上' in sign:
            offset_num = sign.count('上')
        elif '下' in sign:
            offset_num = -sign.count('下')
        elif '本' in sign or '当' in sign or '°0' in sign:
            offset_num = 0
        else:
            raise ValueError(f'时间偏移映射器：未找到时间偏移量 {sign}')

    if isinstance(source, int):
        assert operating_unit == 'Y', '时间偏移映射器：整数只能用于年份偏移'
        mapper = {source: source - offset_num}

    elif isinstance(source, pd.Series):
        assert operating_unit == 'Y', '时间偏移映射器：单列时间只能用于年份偏移'
        source = source.dropna().drop_duplicates().sort_values().copy()
        source.index = source
        suffix = f'{operating_unit}{direction}{abs(offset_num)}'
        mapper = source - offset_num
        mapper.name = mapper.index.name+f'_{suffix}' if mapper.index.name else None
    
    elif isinstance(source, pd.DataFrame):
        year_col = unit_col_info.get('year_col', None)
        quarter_col = unit_col_info.get('quarter_col', None)
        month_col = unit_col_info.get('month_col', None)

        exist_cols = [col for col in [year_col, quarter_col, month_col] if col in source.columns.to_list()]
        source = source[exist_cols].dropna().drop_duplicates().sort_values(exist_cols).copy()
        source = source.set_index(source.columns.tolist()).index.to_frame()

        if operating_unit == 'Y':
            assert year_col in exist_cols, '时间偏移映射器：无年份列'
            source[year_col] -= offset_num

        elif operating_unit == 'Q':
            assert year_col in exist_cols, '时间偏移映射器：无年份列'
            assert quarter_col in exist_cols, '时间偏移映射器：无季度列'
            source[quarter_col] -= offset_num
            source[year_col] += (source[quarter_col] - 1) // 4  # 对应年度偏移
            source[quarter_col] = (source[quarter_col] - 1) % 4 + 1 # 修正季度偏移格式：1-4
            
        elif operating_unit == 'M':
            assert year_col in exist_cols, '时间偏移映射器：无年份列'
            assert month_col in exist_cols, '时间偏移映射器：无月份列'
            source[month_col] -= offset_num
            source[year_col] += (source[month_col] - 1) // 12
            source[month_col] = (source[month_col] - 1) % 12 + 1
            if quarter_col in exist_cols:
                source[quarter_col] = (source[month_col] - 1) // 3 % 4 + 1

        elif operating_unit == 'YEND':
            assert year_col in exist_cols, '时间偏移映射器：无年份列'
            assert quarter_col in exist_cols, '时间偏移映射器：无季度列'
            source[quarter_col] = 4
            source[year_col] -= offset_num

        suffix = f'{operating_unit}{direction}{abs(offset_num)}'
        source.columns = [f"{col}_{suffix}" for col in source.columns]
        mapper = source

    return mapper