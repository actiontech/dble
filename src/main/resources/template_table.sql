-- testdb schema for template only
-- @since 2019-04-26
-- @author yanhuqing666
--
use testdb;
drop table if exists tb_enum_sharding;
create table if not exists tb_enum_sharding (
  id int not null,
  code int not null,
  content varchar(250) not null,
  primary key(id)
)engine=innodb charset=utf8;
insert into tb_enum_sharding values(1,10000,'1'),(2,10010,'2'),(3,10000,'3'),(4,10010,'4');

drop table if exists tb_range_sharding;
create table if not exists tb_range_sharding (
  id int not null,
  content varchar(250) not null,
  primary key(id)
)engine=innodb charset=utf8;
insert into tb_range_sharding values(1,'1'),(5000001,'5000001'),(10000001,'10000001');

drop table if exists tb_hash_sharding;
create table if not exists tb_hash_sharding (
  id int not null,
  id2 int not null,
  content varchar(250) not null,
  primary key(id)
)engine=innodb charset=utf8;
insert into tb_hash_sharding values(1,1,'1'),(2,2,'2'),(513,513,'513');

drop table if exists tb_hash_sharding_er1;
create table if not exists tb_hash_sharding_er1 (
  id int not null,
  id2 int not null,
  content varchar(250) not null,
  primary key(id)
)engine=innodb charset=utf8;
insert into tb_hash_sharding_er1 values(1,1,'1'),(2,2,'2'),(513,513,'513');

drop table if exists tb_hash_sharding_er2;
create table if not exists tb_hash_sharding_er2 (
  id int not null,
  id2 bigint not null,
  content varchar(250) not null,
  primary key(id)
)engine=innodb charset=utf8;
insert into tb_hash_sharding_er2 values(1,1,'1'),(2,2,'2'),(513,513,'513');

drop table if exists tb_hash_sharding_er3;
create table if not exists tb_hash_sharding_er3 (
  id int not null,
  id2 bigint not null,
  content varchar(250) not null,
  primary key(id)
)engine=innodb charset=utf8;
insert into tb_hash_sharding_er3(id,content) values(1,'1'),(2,'2'),(513,'513');

drop table if exists tb_uneven_hash;
create table if not exists tb_uneven_hash (
  id int not null,
  content varchar(250) not null,
  primary key(id)
)engine=innodb charset=utf8;
insert into tb_uneven_hash values(1,'1'),(257,'257'),(513,'513');

drop table if exists tb_mod;
create table if not exists tb_mod (
  id int not null,
  content varchar(250) not null,
  primary key(id)
)engine=innodb charset=utf8;
insert into tb_mod values(1,'1'),(2,'2'),(3,'3'),(4,'4');

drop table if exists tb_jump_hash;
create table if not exists tb_jump_hash (
  id int not null,
  code varchar(250) not null,
  content varchar(250) not null,
  primary key(id)
)engine=innodb charset=utf8;
insert into tb_jump_hash values(1,'1','1'),(2,'2','2'),(3,'3','3'),(4,'4','4');

drop table if exists tb_hash_string;
create table if not exists tb_hash_string (
  id int not null,
  code varchar(250) not null,
  content varchar(250) not null,
  primary key(id)
)engine=innodb charset=utf8;
insert into tb_hash_string values(1,'1','1'),(2,'2','2'),(3,'3','3'),(4,'4','4');

drop table if exists tb_date;
create table if not exists tb_date (
  id int not null,
  code varchar(250) not null,
  create_date date not null,
  primary key(id)
)engine=innodb charset=utf8;
insert into tb_date values(1,'1','2015-01-01'),(2,'2','2015-01-11'),(3,'3','2015-01-21');

drop table if exists tb_pattern;
create table if not exists tb_pattern (
  id int not null,
  content varchar(250) not null,
  primary key(id)
)engine=innodb charset=utf8;
insert into tb_pattern values(1,'1'),(11,'11');

-- global tables
drop table if exists tb_global1;
create table if not exists tb_global1 (
  id int not null,
  content varchar(250) not null,
  primary key(id)
)engine=innodb charset=utf8;
insert into tb_global1 values(1,'1'),(2,'2');

drop table if exists tb_global2;
create table if not exists tb_global2 (
  id int not null,
  content varchar(250) not null,
  primary key(id)
)engine=innodb charset=utf8;
insert into tb_global2 values(1,'1'),(2,'2');

drop table if exists tb_single;
create table if not exists tb_single (
  id int not null,
  content varchar(250) not null,
  primary key(id)
)engine=innodb charset=utf8;
insert into tb_single values(1,'1'),(2,'2');

drop table if exists tb_parent;
create table if not exists tb_parent (
  id int not null,
  id2 int not null,
  content varchar(250) not null,
  primary key(id)
)engine=innodb charset=utf8;
insert into tb_parent values(1,1,'1'),(2,2,'2'),(513,513,'513');

drop table if exists tb_child1;
create table if not exists tb_child1 (
  id int not null,
  child1_id int not null,
  child1_id2 int not null,
  content varchar(250) not null,
  primary key(id)
)engine=innodb charset=utf8;
insert into tb_child1 values(1,1,1,'1');
insert into tb_child1 values(2,2,2,'2');
insert into tb_child1 values(513,513,513,'513');

drop table if exists tb_grandson1;
create table if not exists tb_grandson1 (
  id int not null,
  grandson1_id int not null,
  content varchar(250) not null,
  primary key(id)
)engine=innodb charset=utf8;
insert into tb_grandson1 values(1,1,'1');
insert into tb_grandson1 values(2,2,'2');
insert into tb_grandson1 values(513,513,'513');

drop table if exists tb_grandson2;
create table if not exists tb_grandson2 (
  id int not null,
  grandson2_id int not null,
  content varchar(250) not null,
  primary key(id)
)engine=innodb charset=utf8;
insert into tb_grandson2 values(1,1,'1');
insert into tb_grandson2 values(2,2,'2');
insert into tb_grandson2 values(513,513,'513');

drop table if exists tb_child2;
create table if not exists tb_child2 (
  id int not null,
  child2_id int not null,
  content varchar(250) not null,
  primary key(id)
)engine=innodb charset=utf8;
insert into tb_child2 values(1,1,'1');
insert into tb_child2 values(2,2,'2');
insert into tb_child2 values(513,513,'513');

drop table if exists tb_child3;
create table if not exists tb_child3 (
  id int not null,
  child3_id int not null,
  content varchar(250) not null,
  primary key(id)
)engine=innodb charset=utf8;
insert into tb_child3 values(1,1,'1');
insert into tb_child3 values(2,2,'2');
insert into tb_child3 values(513,513,'513');

use testdb2;
drop table if exists tb_test1;
create table if not exists tb_test1 (
  id int not null,
  content varchar(250) not null,
  primary key(id)
)engine=innodb charset=utf8;
insert into tb_test1 values(1,'1'),(2,'2');

drop table if exists tb_test2;
create table if not exists tb_test2 (
  id int not null,
  content varchar(250) not null,
  primary key(id)
)engine=innodb charset=utf8;
insert into tb_test2 values(1,'1'),(2,'2');
