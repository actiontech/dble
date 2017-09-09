# Roadmap

This document defines the roadmap for dble development.
##### __SQL Layer__  
- [x] DDL
- [x] Query Plan
- [x] Distributed Transactions
- [ ] High Performance LIMIT
- [x] REPLACE Syntax
- [x] Join (LEFT JOIN / RIGHT JOIN / CROSS JOIN)
- [x] Union
- [x] Simple Sub-query
- [ ] Correlated Sub-Query 
- [x] Functions support 
	- [x] Type Conversion in Expression Evaluation
	- [x] Operators
	- [x] Control Flow Functions
	- [x] String Functions     
	- [x] Numeric Functions and Operators     
	- [x] Date and Time Functions
	- [x] Cast Functions and Operators
	- [x] Bit Functions and Operators  
	- [x] Aggregate (GROUP BY) Functions  
- [ ] VIEW
- [ ] allowMultiQueries
- [ ] Charset
- [ ] allowMultiQueries 
- [ ] System Variables 
- [ ] User Variables
- [ ] Kill statement
- [x] Show full tables
- [ ] MySQL5.7 Client/Server Protocol 

##### __Cluster__  
- [x] Zookeeper
- [ ] Consul

##### __Backend Connection Pool__ 
- [x] Correct Idle Conn Heartbeat
- [ ] Connection Version
- [ ] Smooth Offline

##### __Optimization__ 
- [x] Global table
- [x] ER table
- [x] Push down Where Filter 
- [x] Simplify Where Filter 
- [x] Simplify SELECT

##### __Remove__ 
- [x] Least frequently used algorithm
- [x] Heterogeneous database
- [x] Migrate Logic
- [x] Other unused Code