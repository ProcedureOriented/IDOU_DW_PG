from typing import List, Tuple, Dict, Set, Optional, Union, Any, TypedDict

class normal_fdict(TypedDict):
    """计算和定参判断公式拆解字典"""
    formula: str
    _type: str

class timecalc_fdict(normal_fdict):
    """时间计算公式拆解字典"""  
    time_unit: str

class stats_fdict(normal_fdict):
    """统计公式拆解字典"""
    stats_method: str
    stats_field: str
    group_keys: List[str]
    condition: str
    add_condition: str  # 旧名称

class paramjudge_fdict(normal_fdict):
    """不定参判断公式拆解字典"""
    target: str
    direction: str
    dimension_names: List[str]