-- tccp schema for test only
-- @since 2019-04-08
-- @author collapsar
--

-- 供销商所属仓库表
drop table if exists warehouse;
create table warehouse (
  id smallint not null comment '仓库id',
  w_name varchar(10) comment '仓库名',
  w_street_1 varchar(20), 
  w_street_2 varchar(20), 
  w_city varchar(20), 
  w_state char(2), 
  w_zip char(9), 
  w_tax decimal(4,2) comment '仓库税',
  w_ytd decimal(12,2),
  primary key (id)
) engine=InnoDB charset=utf8;

-- 区域销售点表
drop table if exists district;
create table district (
  id tinyint not null comment '区域销售点id',
  d_w_id smallint not null comment '直接供货仓库id',
  d_name varchar(10) comment '销售点称',
  d_street_1 varchar(20), 
  d_street_2 varchar(20), 
  d_city varchar(20), 
  d_state char(2), 
  d_zip char(9), 
  d_tax decimal(4,2) comment '仓库税',
  d_ytd decimal(12,2),
  primary key (d_w_id, id)
) engine=InnoDB charset=utf8;

-- 客户表
drop table if exists customer;
create table customer (
  id int not null comment '客户id',
  c_d_id tinyint not null comment '销售点id',
  c_w_id smallint not null comment '仓库id',
  c_first varchar(16), 
  c_middle char(2), 
  c_last varchar(16) comment '客户名',
  c_street_1 varchar(20), 
  c_street_2 varchar(20), 
  c_city varchar(20), 
  c_state char(2), 
  c_zip char(9), 
  c_phone char(16), 
  c_since datetime,
  c_credit char(2) comment '信用卡状态',
  c_credit_lim bigint, 
  c_discount decimal(4,2) comment '折扣率',
  c_balance decimal(12,2) comment '余额',
  c_ytd_payment decimal(12,2), 
  c_payment_cnt smallint, 
  c_delivery_cnt smallint, 
  c_data text,
  PRIMARY KEY(c_w_id, c_d_id, id)
) engine=InnoDB charset=utf8;

-- 客户历史记录表
drop table if exists history;
create table history (
  h_c_id int, 
  h_c_d_id tinyint, 
  h_c_w_id smallint,
  h_d_id tinyint,
  h_w_id smallint,
  h_date datetime,
  h_amount decimal(6,2), 
  h_data varchar(24)
) engine=InnoDB charset=utf8;

-- 新订单表
drop table if exists new_orders;
create table new_orders (
  no_o_id int not null comment '订单id',
  no_d_id tinyint not null comment '销售点id',
  no_w_id smallint not null comment '仓库id',
  PRIMARY KEY(no_w_id, no_d_id, no_o_id)
) engine=InnoDB charset=utf8;

-- 订单表
drop table if exists orders;
create table orders (
  id int not null comment '订单id',
  o_d_id tinyint not null comment '销售点id',
  o_w_id smallint not null comment '仓库id',
  o_c_id int comment '客户id',
  o_entry_d datetime comment '下单日期',
  o_carrier_id tinyint comment '批次号',
  o_ol_cnt tinyint comment '订单货物种类数',
  o_all_local tinyint comment '是否本地供货',
  PRIMARY KEY(o_w_id, o_d_id, id)
) engine=InnoDB charset=utf8;

-- 订单明细表
drop table if exists order_line;
create table order_line ( 
  ol_o_id int not null comment '订单id',
  ol_d_id tinyint not null comment '销售点id',
  ol_w_id smallint not null comment '仓库id',
  ol_number tinyint not null comment '订单货物种类数量',
  ol_i_id int comment '货物id',
  ol_supply_w_id smallint comment '供货仓库id',
  ol_delivery_d datetime comment '发货日期',
  ol_quantity tinyint comment '订购数量',
  ol_amount decimal(6,2) comment '货物总价，未计算折扣价',
  ol_dist_info char(24),
  PRIMARY KEY(ol_w_id, ol_d_id, ol_o_id, ol_number)
) engine=InnoDB charset=utf8;

-- 货物表
drop table if exists item;
create table item (
  id int not null comment '货物id',
  i_im_id int, 
  i_name varchar(24) comment '货物名',
  i_price decimal(5,2) comment '货物单价',
  i_data varchar(50),
  PRIMARY KEY(id)
) engine=InnoDB charset=utf8;

-- 库存表
drop table if exists stock;
create table stock (
  s_i_id int not null comment '货物id',
  s_w_id smallint not null comment '仓库id',
  s_quantity smallint comment '库存量',
  s_dist_01 char(24), 
  s_dist_02 char(24),
  s_dist_03 char(24),
  s_dist_04 char(24), 
  s_dist_05 char(24), 
  s_dist_06 char(24), 
  s_dist_07 char(24), 
  s_dist_08 char(24), 
  s_dist_09 char(24), 
  s_dist_10 char(24), 
  s_ytd decimal(8,0), 
  s_order_cnt smallint comment '订单量',
  s_remote_cnt smallint comment '远程供货次数',
  s_data varchar(50),
  PRIMARY KEY(s_w_id, s_i_id)
) engine=InnoDB charset=utf8;