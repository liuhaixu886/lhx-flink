CREATE DATABASE IF NOT EXISTS realtime_config;

CREATE TABLE IF NOT EXISTS realtime_config.table_process_dim (
  source_table varchar(200) NOT NULL COMMENT '来源业务表名',
  sink_table varchar(200) NOT NULL COMMENT 'HBase目标表名',
  sink_family varchar(200) NOT NULL DEFAULT 'info' COMMENT 'HBase列族',
  sink_row_key varchar(200) NOT NULL DEFAULT 'id' COMMENT 'HBase rowkey字段',
  sink_columns varchar(1000) DEFAULT NULL COMMENT '写入HBase的字段，逗号分隔，为空表示写全部字段',
  sink_type varchar(20) NOT NULL DEFAULT 'dim' COMMENT '目标类型，固定dim',
  PRIMARY KEY (source_table)
) COMMENT='实时数仓DIM层维表处理配置';

INSERT INTO realtime_config.table_process_dim
(source_table, sink_table, sink_family, sink_row_key, sink_columns, sink_type)
VALUES
-- 地区维度
('base_province', 'dim_base_province', 'info', 'id', 'name,region_id,area_code,iso_code,iso_3166_2', 'dim'),
('base_region', 'dim_base_region', 'info', 'id', 'region_name', 'dim'),

-- 商品维度
('base_attr_info', 'dim_base_attr_info', 'info', 'id', 'attr_name,category_id,category_level', 'dim'),
('base_attr_value', 'dim_base_attr_value', 'info', 'id', 'value_name,attr_id', 'dim'),
('base_category1', 'dim_base_category1', 'info', 'id', 'name', 'dim'),
('base_category2', 'dim_base_category2', 'info', 'id', 'name,category1_id', 'dim'),
('base_category3', 'dim_base_category3', 'info', 'id', 'name,category2_id', 'dim'),
('base_sale_attr', 'dim_base_sale_attr', 'info', 'id', 'name', 'dim'),
('base_trademark', 'dim_base_trademark', 'info', 'id', 'tm_name,logo_url', 'dim'),
('spu_info', 'dim_spu_info', 'info', 'id', 'spu_name,description,category3_id,tm_id', 'dim'),
('spu_image', 'dim_spu_image', 'info', 'id', 'spu_id,img_name,img_url', 'dim'),
('spu_sale_attr', 'dim_spu_sale_attr', 'info', 'id', 'spu_id,base_sale_attr_id,sale_attr_name', 'dim'),
('spu_sale_attr_value', 'dim_spu_sale_attr_value', 'info', 'id', 'spu_id,base_sale_attr_id,sale_attr_value_name,sale_attr_name', 'dim'),
('sku_info', 'dim_sku_info', 'info', 'id', 'spu_id,price,sku_name,sku_desc,weight,tm_id,category3_id,sku_default_img,is_sale,create_time', 'dim'),
('sku_attr_value', 'dim_sku_attr_value', 'info', 'id', 'attr_id,value_id,sku_id,attr_name,value_name', 'dim'),
('sku_image', 'dim_sku_image', 'info', 'id', 'sku_id,img_name,img_url,spu_img_id,is_default', 'dim'),
('sku_sale_attr_value', 'dim_sku_sale_attr_value', 'info', 'id', 'sku_id,spu_id,sale_attr_value_id,sale_attr_id,sale_attr_name,sale_attr_value_name', 'dim'),

-- 字典和用户维度
('base_dic', 'dim_base_dic', 'info', 'dic_code', 'dic_name,parent_code,create_time,operate_time', 'dim'),
('user_info', 'dim_user_info', 'info', 'id', 'login_name,nick_name,passwd,name,phone_num,email,head_img,user_level,birthday,gender,create_time,operate_time,status', 'dim'),

-- 营销维度
('activity_info', 'dim_activity_info', 'info', 'id', 'activity_name,activity_type,activity_desc,start_time,end_time,create_time', 'dim'),
('activity_rule', 'dim_activity_rule', 'info', 'id', 'activity_id,activity_type,condition_amount,condition_num,benefit_amount,benefit_discount,benefit_level', 'dim'),
('activity_sku', 'dim_activity_sku', 'info', 'id', 'activity_id,sku_id,create_time', 'dim'),
('coupon_info', 'dim_coupon_info', 'info', 'id', 'coupon_name,coupon_type,condition_amount,condition_num,activity_id,benefit_amount,benefit_discount,create_time,range_type,limit_num,taken_count,start_time,end_time,operate_time,expire_time,range_desc', 'dim'),
('coupon_range', 'dim_coupon_range', 'info', 'id', 'coupon_id,range_type,range_id', 'dim')
ON DUPLICATE KEY UPDATE
  sink_table = VALUES(sink_table),
  sink_family = VALUES(sink_family),
  sink_row_key = VALUES(sink_row_key),
  sink_columns = VALUES(sink_columns),
  sink_type = VALUES(sink_type);


SELECT source_table, sink_table, sink_row_key, sink_columns
FROM realtime_config.table_process_dim
WHERE source_table IN ('base_province','sku_info','base_dic','user_info');



UPDATE gmall.base_province
SET iso_code = CONCAT(iso_code, '_test')
WHERE id = 1;

UPDATE gmall.base_province
SET iso_code = REPLACE(iso_code, '_test', '')
WHERE id = 1;
