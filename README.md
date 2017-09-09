# DBLE

## What is DBLE?

DBLE is a high scalability middle-ware for MySQL sharding.


- __Sharding__
As your business grows, you can use DBLE to replace the origin single MySQL instance. 

- __Compatible with MySQL protocol__
Use DBLE as MySQL. You can replace MySQL with DBLE to power your application  without changing a single line of code in most cases.

- __High availability__
DBLE server can used as clustered,business will not suffer with single node fail.

- __SQL Support__
Support(some in Roadmap) SQL 92 standard and MySQL dialect. We support complex SQL query like group by, order by, distinct ,join ,union, subquery(in Roadmap) and so on.

- __Complex Query Optimize__
Optimize the complex query, including, without limitation, Global-table join sharding-table，ER-relation tables，Subquerys，Simplifying select items，and the like.

- __Distributed Transaction__
Support Distributed Transaction using  two-phase commit. You can choose normal mode for performance or XA mode for data safe，of cource，the XA mode dependent on MySQL5.7's XA Transaction, MySQL node's high availability and data reliability of disk.


## History
Firstly,hanks for [MyCat](https://github.com/MyCATApache/Mycat-Server)'s contribution in the open source community.  
But for us, focusing more attention on support for MySQL is a better choise.So we removed the support for other heterogeneous database,deeply improved/optimized the compatible, complex query and distributed transaction. And of cource, fixed some bugs during testing.

For more details, see[Roadmap](./docs/ROADMAP.md) or [Issues](https://github.com/actiontech/dble/issues) . 

## Roadmap

Read the [Roadmap](./docs/ROADMAP.md).

## Architecture

![architecture](./docs/architecture.png)

## Quick start 
Read the [Quick Start](./docs/QUICKSTART.md).


## Contributing

Contributions are welcomed and greatly appreciated. See [CONTRIBUTING.md](./docs/CONTRIBUTING.md)
for details on submitting patches and the contribution workflow.



