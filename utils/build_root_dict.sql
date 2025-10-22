-- Active: 1760805643746@@127.0.0.1@55432@dwbj
-- postgresql

CREATE OR REPLACE FUNCTION set_update_at()
RETURNS TRIGGER AS $$
BEGIN
-- 若未手动指定 update_at（值为 NULL 或未提供），则自动填充当前时间
-- 注意：在 BEFORE UPDATE 触发器中无法区分“未提供”与“提供且值与原值相同”的情形，
-- 因此把 NEW.update_at IS NULL OR NEW.update_at = OLD.update_at 视为未手动指定的情况。
IF NEW.update_at IS NULL OR NEW.update_at = OLD.update_at THEN
    NEW.update_at := NOW();
END IF;
RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 全数据字典-表清单
CREATE TABLE IF NOT EXISTS public.r_dict_table_info (
    id numeric(6, 2) NOT NULL,
    db_segment varchar(50) NOT NULL,
    group1 varchar(20) NULL,
    group2 varchar(20) NULL,
    group3 varchar(20) NULL,
    table_code varchar(60) NOT NULL,
    table_name varchar(60) NOT NULL,
    sync_code varchar(60) NULL,
    sync_name varchar(60) NULL,
    use_ashare bool NULL,
    use_bond bool NULL,
    use_hk bool NULL,
    use_neeq bool NULL,
    data_nature_freq varchar(20) NULL,
    data_update_freq varchar(20) NULL,
    remarks text NULL,
    create_at timestamp(0) DEFAULT CURRENT_TIMESTAMP NOT NULL,
    update_at timestamp(0) DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT r_dict_table_info_pk PRIMARY KEY (id),
    CONSTRAINT r_dict_table_info_table_code_uq UNIQUE (table_code)
);
COMMENT ON TABLE public.r_dict_table_info IS '全数据字典-表清单';

COMMENT ON COLUMN public.r_dict_table_info.id IS '展示顺序';
COMMENT ON COLUMN public.r_dict_table_info.db_segment IS '表性质';
COMMENT ON COLUMN public.r_dict_table_info.group1 IS '顶级数据分区';
COMMENT ON COLUMN public.r_dict_table_info.group2 IS '表二级分组';
COMMENT ON COLUMN public.r_dict_table_info.group3 IS '表三级分组';
COMMENT ON COLUMN public.r_dict_table_info.table_code IS '理论表代码';
COMMENT ON COLUMN public.r_dict_table_info.table_name IS '理论表名';
COMMENT ON COLUMN public.r_dict_table_info.sync_code IS '实际数据库表代码';
COMMENT ON COLUMN public.r_dict_table_info.sync_name IS '实际数据库表名';
COMMENT ON COLUMN public.r_dict_table_info.use_ashare IS '适用_A股';
COMMENT ON COLUMN public.r_dict_table_info.use_bond IS '适用_发债';
COMMENT ON COLUMN public.r_dict_table_info.use_hk IS '适用_港股';
COMMENT ON COLUMN public.r_dict_table_info.use_neeq IS '适用_新三板';
COMMENT ON COLUMN public.r_dict_table_info.data_nature_freq IS '数据性质频率';
COMMENT ON COLUMN public.r_dict_table_info.data_update_freq IS '运营更新频率';
COMMENT ON COLUMN public.r_dict_table_info.remarks IS '备注';
COMMENT ON COLUMN public.r_dict_table_info.create_at IS '创建时间';
COMMENT ON COLUMN public.r_dict_table_info.update_at IS '更新时间';

CREATE TRIGGER r_dict_table_info_update BEFORE 
    UPDATE ON public.r_dict_table_info 
    FOR EACH ROW EXECUTE FUNCTION set_update_at();

-- 全数据字典-字段清单
CREATE TABLE IF NOT EXISTS public.r_dict_field_info (
    table_code varchar(60) NOT NULL,
    table_name varchar(60) NOT NULL,
    field_order int4 NOT NULL,
    field_code varchar(60) NOT NULL,
    field_name varchar(60) NOT NULL,
    data_type varchar(60) NULL,
    data_type_para varchar(60) NULL,
    default_value varchar(255) NULL,
    is_not_null bool DEFAULT false NOT NULL,
    field_hierarchy varchar(10) NULL,
    enable_status varchar(20) NULL,
    sync_field_code varchar(255) NULL,
    history_code varchar(255) NULL,
    sync_is_label_value int2 NULL,
    remarks text NULL,
    create_at timestamp(0) DEFAULT CURRENT_TIMESTAMP NOT NULL,
    update_at timestamp(0) DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT r_dict_field_info_pk PRIMARY KEY (table_code, field_code),
    CONSTRAINT r_dict_field_info_table_info_fk FOREIGN KEY (table_code) REFERENCES public.r_dict_table_info(table_code) ON DELETE RESTRICT ON UPDATE CASCADE
);
COMMENT ON TABLE public.r_dict_field_info IS '全数据字典-字段清单';

COMMENT ON COLUMN public.r_dict_field_info.table_code IS '理论表代码';
COMMENT ON COLUMN public.r_dict_field_info.table_name IS '理论表名';
COMMENT ON COLUMN public.r_dict_field_info.field_order IS '表内顺序';
COMMENT ON COLUMN public.r_dict_field_info.field_code IS '理论字段代码';
COMMENT ON COLUMN public.r_dict_field_info.field_name IS '中文名称';
COMMENT ON COLUMN public.r_dict_field_info.data_type IS '数据类型';
COMMENT ON COLUMN public.r_dict_field_info.data_type_para IS '数据类型参数';
COMMENT ON COLUMN public.r_dict_field_info.default_value IS '默认值';
COMMENT ON COLUMN public.r_dict_field_info.is_not_null IS '要求非空';
COMMENT ON COLUMN public.r_dict_field_info.field_hierarchy IS '数据层级';
COMMENT ON COLUMN public.r_dict_field_info.enable_status IS '启用状态';
COMMENT ON COLUMN public.r_dict_field_info.sync_field_code IS '数据库实际代码';
COMMENT ON COLUMN public.r_dict_field_info.history_code IS '旧架构代码';
COMMENT ON COLUMN public.r_dict_field_info.sync_is_label_value IS '长表值';
COMMENT ON COLUMN public.r_dict_field_info.remarks IS '备注';
COMMENT ON COLUMN public.r_dict_field_info.create_at IS '创建时间';
COMMENT ON COLUMN public.r_dict_field_info.update_at IS '更新时间';

CREATE TRIGGER r_dict_field_info_update BEFORE 
    UPDATE ON public.r_dict_field_info 
    FOR EACH ROW EXECUTE FUNCTION set_update_at();


-- 全数据字典-字段来源清单
CREATE TABLE IF NOT EXISTS public.r_dict_field_sources (
    table_code varchar(60) NOT NULL,
    field_code varchar(60) NOT NULL,
    source_order int4 NOT NULL,
    source_channel varchar(60) NOT NULL,
    source_table varchar(60) NOT NULL,
    source_field varchar(60) NOT NULL,
    channel_status varchar(20) NULL,
    filter_condition text NULL,
    fetch_comment text NULL,
    source_data_type varchar(60) NULL,
    source_data_format varchar(60) NULL,
    source_unit varchar(60) NULL,
    source_missing_flag varchar(20) NULL,
    is_need_transform bool NULL,
    transform_rule text NULL,
    create_at timestamp(0) DEFAULT CURRENT_TIMESTAMP NOT NULL,
    update_at timestamp(0) DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT r_dict_field_sources_pk PRIMARY KEY (table_code, field_code, source_order),
    CONSTRAINT r_dict_field_sources_field_info_fk FOREIGN KEY (table_code, field_code) REFERENCES public.r_dict_field_info(table_code, field_code) ON DELETE RESTRICT ON UPDATE CASCADE
);
COMMENT ON TABLE public.r_dict_field_sources IS '全数据字典-字段来源清单';

COMMENT ON COLUMN public.r_dict_field_sources.table_code IS '理论表代码';
COMMENT ON COLUMN public.r_dict_field_sources.field_code IS '理论字段代码';
COMMENT ON COLUMN public.r_dict_field_sources.source_order IS '来源顺序';
COMMENT ON COLUMN public.r_dict_field_sources.source_channel IS '来源渠道';
COMMENT ON COLUMN public.r_dict_field_sources.source_table IS '来源表';
COMMENT ON COLUMN public.r_dict_field_sources.source_field IS '来源字段';
COMMENT ON COLUMN public.r_dict_field_sources.channel_status IS '渠道状态';
COMMENT ON COLUMN public.r_dict_field_sources.filter_condition IS '同步时过滤条件';
COMMENT ON COLUMN public.r_dict_field_sources.fetch_comment IS '同步备注';
COMMENT ON COLUMN public.r_dict_field_sources.source_data_type IS '来源数据类型';
COMMENT ON COLUMN public.r_dict_field_sources.source_data_format IS '来源数据格式';
COMMENT ON COLUMN public.r_dict_field_sources.source_unit IS '来源单位';
COMMENT ON COLUMN public.r_dict_field_sources.source_missing_flag IS '源缺失标识';
COMMENT ON COLUMN public.r_dict_field_sources.is_need_transform IS '是否需要转换';
COMMENT ON COLUMN public.r_dict_field_sources.transform_rule IS '转换规则';
COMMENT ON COLUMN public.r_dict_field_sources.create_at IS '创建时间';
COMMENT ON COLUMN public.r_dict_field_sources.update_at IS '更新时间';

CREATE TRIGGER r_dict_field_sources_update BEFORE 
    UPDATE ON public.r_dict_field_sources 
    FOR EACH ROW EXECUTE FUNCTION set_update_at();

-- 全数据字典-表约束
CREATE TABLE IF NOT EXISTS public.r_dict_table_constraints (
    owner_table varchar(60) NOT NULL,
    constraint_name varchar(60) NOT NULL,
    constraint_type varchar(10) NOT NULL,
    fk_ref_to varchar(60) NOT NULL,
    pos01 varchar(60) NULL,
    pos02 varchar(60) NULL,
    pos03 varchar(60) NULL,
    pos04 varchar(60) NULL,
    pos05 varchar(60) NULL,
    pos06 varchar(60) NULL,
    pos07 varchar(60) NULL,
    pos08 varchar(60) NULL,
    pos09 varchar(60) NULL,
    pos10 varchar(60) NULL,
    fk_limit varchar(255) NULL,
    create_at timestamp(0) DEFAULT CURRENT_TIMESTAMP NOT NULL,
    update_at timestamp(0) DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT r_dict_table_constraints_pk PRIMARY KEY (constraint_name, fk_ref_to),
    CONSTRAINT r_dict_table_constraints_fk_ref_to_fk FOREIGN KEY (fk_ref_to) REFERENCES public.r_dict_table_info(table_code) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT r_dict_table_constraints_owner_table_fk FOREIGN KEY (owner_table) REFERENCES public.r_dict_table_info(table_code) ON DELETE RESTRICT ON UPDATE CASCADE
);
COMMENT ON TABLE public.r_dict_table_constraints IS '全数据字典-表约束';

COMMENT ON COLUMN public.r_dict_table_constraints.owner_table IS '所属表代码';
COMMENT ON COLUMN public.r_dict_table_constraints.constraint_name IS '约束名称';
COMMENT ON COLUMN public.r_dict_table_constraints.constraint_type IS '约束或索引类型: pk-主键，uq-唯一约束，fk-外键，idx-索引';
COMMENT ON COLUMN public.r_dict_table_constraints.fk_ref_to IS '外键引用表';
COMMENT ON COLUMN public.r_dict_table_constraints.pos01 IS '位置1';
COMMENT ON COLUMN public.r_dict_table_constraints.pos02 IS '位置2';
COMMENT ON COLUMN public.r_dict_table_constraints.pos03 IS '位置3';
COMMENT ON COLUMN public.r_dict_table_constraints.pos04 IS '位置4';
COMMENT ON COLUMN public.r_dict_table_constraints.pos05 IS '位置5';
COMMENT ON COLUMN public.r_dict_table_constraints.pos06 IS '位置6';
COMMENT ON COLUMN public.r_dict_table_constraints.pos07 IS '位置7';
COMMENT ON COLUMN public.r_dict_table_constraints.pos08 IS '位置8';
COMMENT ON COLUMN public.r_dict_table_constraints.pos09 IS '位置9';
COMMENT ON COLUMN public.r_dict_table_constraints.pos10 IS '位置10';
COMMENT ON COLUMN public.r_dict_table_constraints.fk_limit IS '外键限制: 当fk_reference为true时必填';
COMMENT ON COLUMN public.r_dict_table_constraints.create_at IS '创建时间';
COMMENT ON COLUMN public.r_dict_table_constraints.update_at IS '更新时间';

CREATE TRIGGER r_dict_table_constraints_update BEFORE 
    UPDATE ON public.r_dict_table_constraints 
    FOR EACH ROW EXECUTE FUNCTION set_update_at();
