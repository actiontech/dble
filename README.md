![dble](./docs/dble_logo.png)

[![GitHub issues](https://img.shields.io/github/issues/actiontech/dble.svg)](https://github.com/actiontech/dble/issues)
[![GitHub closed issues](https://img.shields.io/github/issues-closed-raw/actiontech/dble.svg)](https://github.com/actiontech/dble/issues?q=is%3Aissue+is%3Aclosed)
[![GitHub forks](https://img.shields.io/github/forks/actiontech/dble.svg)](https://github.com/actiontech/dble/network/members)
[![GitHub stars](https://img.shields.io/github/stars/actiontech/dble.svg)](https://github.com/actiontech/dble/stargazers)
[![GitHub contributors](https://img.shields.io/github/contributors/actiontech/dble.svg)](https://github.com/actiontech/dble/graphs/contributors)
[![GitHub license](https://img.shields.io/github/license/actiontech/dble.svg)](https://github.com/actiontech/dble/blob/master/LICENSE)

[![Code Quality: Java](https://img.shields.io/lgtm/grade/java/github/actiontech/dble.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/actiontech/dble/context:java)
[![Travis-ci build](https://img.shields.io/travis/com/actiontech/dble/master.svg)](https://travis-ci.com/actiontech?tab=insights)
[![GitHub release](https://img.shields.io/github/release/actiontech/dble.svg)](https://github.com/actiontech/dble/releases) 
[![GitHub download](https://img.shields.io/github/downloads/actiontech/dble/total.svg)](https://github.com/actiontech/dble/releases) 
[![status](https://img.shields.io/badge/status-working%20in%20banks-blue.svg)](https://github.com/actiontech/dble)

dble (pronouced "double", less bug and no "ou") is maintained by [ActionTech](https://opensource.actionsky.com).

## What is dble?

dble is a high scalability middle-ware for MySQL sharding. 

- __Sharding__
As your business grows, you can use dble to replace the origin single MySQL instance. 

- __Compatible with MySQL protocol__
Use dble as same as MySQL. You can replace MySQL with dble to power your application without changing a single line of code in most cases.

- __High availability__
dble server can be used as clustered, business will not suffer from single node fail.

- __SQL Support__
Support(some in Roadmap) SQL 92 standard and MySQL dialect. We support complex SQL query like group by, order by, distinct, join ,union, sub-query(in Roadmap) and so on.

- __Complex Query Optimization__
Optimize the complex query, including, without limitation, Global-table join sharding-table, ER-relation tables, Sub-Queries, Simplifying select items, and the like.

- __Distributed Transaction__
Support Distributed Transaction using two-phase commit. You can choose normal mode for performance or XA mode for data safety, of course, the XA mode dependent on MySQL-5.7's XA Transaction, MySQL node's high availability and data reliability of disk.


## History
dble is based on [MyCat](https://github.com/MyCATApache/Mycat-Server). First of all,  thanks to the contributors from MyCat.

For us, focusing on MySQL is a better choice. So we cancelled the support for other databases, deeply improved/optimized its behavior on compatibility, complex query and distributed transaction. And of course, fixed lots of bugs.

For more details, see [Roadmap](./docs/ROADMAP.md) and [Issues](https://github.com/actiontech/dble/issues) . 

## Roadmap

Read the [Roadmap](./docs/ROADMAP.md).

## Architecture

![architecture](./docs/architecture.PNG)

## Quick start 
Read the [Quick Start](./docs/QUICKSTART.md) or [Quick Start With Docker](./docs/dble_quick_start_docker.md) or  [Quick Start With Docker-Compose](./docs/dble_start_docker_compose.md).  

参见文档[快速开始](https://github.com/actiontech/dble-docs-cn/blob/master/0.overview/0.3_dble_quick_start.md)或者[Docker快速开始](https://github.com/actiontech/dble-docs-cn/blob/master/0.overview/0.4_dble_quick_start_docker.md)或者[Docker-Compose快速开始](https://github.com/actiontech/dble-docs-cn/blob/master/0.overview/0.5_dble_start_docker_compose.md).

## Official website
For more information, please visit the [official website](https://opensource.actionsky.com).

## Documentation
+ [简体中文](https://actiontech.github.io/dble-docs-cn/)
+ [中文公开课](https://opensource.actionsky.com/dble-lessons/)
+ English(Comming soon)

## Contributing

Contributions are welcomed and greatly appreciated. See [CONTRIBUTING.md](./docs/CONTRIBUTING.md)
for details on submitting patches and the contribution workflow.

## Community

* IRC: [![Visit our IRC channel](https://kiwiirc.com/buttons/irc.freenode.net/dble.png)](https://kiwiirc.com/client/irc.freenode.net/?nick=user|?&theme=cli#dble)
* QQ group: 669663113
* [If you're using DBLE, please let us know.](https://wj.qq.com/s/2291106/09f4)
* wechat subscription QR code
  
  ![dble](./docs/QR_code.png)

## Contact us

Dble has enterprise support plans, you may contact our sales team: 
* Global Sales: 400-820-6580
* North China: 86-13718877200, Mr.Wang
* South China: 86-18503063188, Mr.Cao
* East China: 86-18930110869, Mr.Liang
* South-West China: 86-13540040119, Mr.Hong
