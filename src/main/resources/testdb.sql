-- testdb schema for test only
-- @since 2018-09-27
-- @author little-pan
--

-- auto sharding by id (long)
create table if not exists travelrecord (
  id bigint not null auto_increment,
  participants varchar(250) not null,
  origin varchar(50) not null,
  destination varchar(50) not null,
  start_time datetime not null,
  end_time datetime not null,
  description text,
  primary key(id)
)engine=innodb charset=utf8;

-- global tables
create table if not exists company (
  id bigint not null auto_increment,
  name varchar(20) not null,
  address varchar(250) not null,
  create_time timestamp not null,
  owner varchar(20) not null,
  employees integer not null default 50,
  primary key(id)
)engine=innodb charset=utf8;

create table if not exists goods (
  id bigint not null auto_increment,
  name varchar(50) not null,
  price decimal(9,2) not null,
  quantity integer not null,
  create_time timestamp not null,
  images varchar(250),
  primary key(id)
)engine=innodb charset=utf8;

-- random sharding
create table if not exists hotnews (
  id bigint not null auto_increment,
  title varchar(50) not null,
  create_time datetime not null,
  author varchar(20) null,
  clicks integer not null default 0,
  content text,
  primary key(id)
)engine=innodb charset=utf8;

-- ER sharding
create table if not exists customer (
  id bigint not null auto_increment,
  name varchar(20) not null,
  birth_date date null,
  birth_day time null,
  height double null,
  weight double null,
  contact varchar(50) null,
  address varchar(80) null,
  hobby varchar(250) null,
  primary key(id)
)engine=innodb charset=utf8;
-- issue
-- mysql> explain select*from customer where id=2;
-- ERROR 1064 (HY000): Can't find a valid data node for specified node index :customer -> ID -> 2 -> Index : 2

create table if not exists orders (
  id bigint not null auto_increment,
  order_no varchar(60) not null,
  order_time datetime not null,
  amount decimal(12, 2) not null,
  customer_id bigint not null,
  primary key(id),
  foreign key(customer_id) references customer(id)
)engine=innodb charset=utf8;

create table if not exists order_items (
  id bigint not null auto_increment,
  goods_id bigint not null,
  goods_name varchar(50) not null,
  price decimal(9, 2) not null,
  quantity integer not null,
  order_id bigint not null,
  primary key(id),
  foreign key(goods_id) references goods(id),
  foreign key(order_id) references orders(id)
)engine=innodb charset=utf8;

create table if not exists customer_addr (
  id bigint not null auto_increment,
  address varchar(80) not null,
  contact varchar(50) not null,
  customer_id bigint not null,
  primary key(id),
  foreign key(customer_id) references customer(id)
)engine=innodb charset=utf8;
