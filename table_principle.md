# 实际数据表
- 横表
- 明确索引
- 不同版本（清洗前清洗后等）分行或者分表
- 明确字段格式

例如：
```SQL
CREATE TABLE base_finance_widetest (
    id int(11) NOT NULL AUTO_INCREMENT ,
    crmcode varchar NOT NULL ,
    stockcode varchar ,
    info_id tinyint(1) NOT NULL COMMENT '1=公众，2=上传，港股不在此表',
    washed tinyint(1) NOT NULL COMMENT '0=原始版本，1=清洗版本',
    artificial tinyint(1) NOT NULL COMMENT '0=自动，1=人工',
    his_order tinyint(2) NOT NULL COMMENT '最新为0，数字越大版本越旧',
    year int(4) NOT NULL ,
    quar tinyint(1) NOT NULL COMMENT '非1-4数值为异常版本编号',
    statement_time date COMMENT '报告期',
    create_time timestamp DEFAULT CURRENT_TIMESTAMP ,
    update_time timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP ,
    
    apturn decimal(18,8) COMMENT '应付账款周转率, APTURN',
    assetstoequity decimal(18,8) COMMENT '权益乘数, ASSETSTOEQUITY',
    baddebtforotherreceivables decimal(18,8) COMMENT '坏账准备-其他应收款, BADDEBTFOROTHERRECEIVABLES',
    
    PRIMARY KEY (id),
    UNIQUE KEY crm_all_IDX (crmcode, info_id, washed, artificial, his_order, year, quar),
    UNIQUE KEY stock_all_IDX (stockcode, info_id, washed, artificial, his_order, year, quar),
    UNIQUE KEY update_time_IDX (update_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
```

# 规范
表按步骤分为：
- 导入表：从模板导入的表，等待抽取到基表
- 基表：基础层的表
- 加工表：中间层加工的表，实际储存
- 视图：中间层创建的视图，不实际储存
  - 执行快、调用频率低、没有复杂操作的表考虑使用视图

字段
- 字段命名尽可能简单
- 字段备注至少包含其中文名，必要时可以添加旧版本使用的代码便于查找
- 在业务字段较少、极少需要加字段的表中，时间戳和软删除，放在最后
  - 与之相对的是，所有财务表的基础字段都应放在开始

约束和索引
- 如果可以确定唯一的行，添加唯一约束
- 建立一个全索引，包含查询经常用到的字段
- 除全索引外尽可能少创建更多索引

# 表命名
表名：
- 在基础层和中间层中，包含分区和表示表本身内容的名称，如 fin_base_balance_sheet，其中fin为一级分区表示财务数据，base为二级分区表示基础三表；
- 在其他层中可以有简单的命名（因为与原始层表并非一对一关系）

原始层（Ancestor）：a_[表名]_[来源]  
基础层（Base）：b_[表名]  
基础层特殊视图：bv_[表名]  
中间层（Completion）：c_[表名]  
应用层（Distribution）：d_[pub/pri]_[下游]_[表名][_mtr，物化视图]  
映射表：m_[映射表名]  
数据字典 、对接规则、优先级规则、加工清单：r_[分类]_[表名]  
加工具体配置：s_[功能]_[表名]  
加工过程临时表：t_[表名]  
前端等系统相关配置：z_[表名]  

# 配置组
表目录  
字段目录  
字段来源映射  
view：表来源映射  

理想表继承实际表，另有一套表目录和字段目录