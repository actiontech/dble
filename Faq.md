# dble-maxConnections  

## ISSUE

- [Err] 3009 - java.io.IOException: the max activeConnnections size can not be max than maxconnections. 

## Resolution

1. 检查是否使用了过多的短链，导致频繁建立连接，链接池不够用 
>**注意**：不要过多的使用短链，容易消耗dble资源  
2.  maxCon配置过小，调大maxCon 

| 配置名称 | 配置内容 | 默认值/单位 | 作用原理或应用 |
| ---- | ---- | ---- | ----|
| maxCon | 控制最大连接数 | 默认1024 | 大于此连接数之后,建立连接会失败.注意当各个用户的maxcon总和值大于此值时，以当前值为准 全局maxCon不作用于manager用户 |

## Root Cause  

配置的链接池数量不够用，导致报错。 

## relevant content  

**数据库连接池**  
1. **maxWait**  
	从连接池获取连接的超时等待时间，单位毫秒。 
> **注意**：maxActive不要配置过大，虽然业务量飙升后还能处理更多的请求，但其实连接数的增多在很多场景下反而会减低吞吐量

2. **connectionProperties**  
	可以配置 connectTimeout 和 socketTimeout，单位都是毫秒。
	connectTimeout 配置建立 TCP 连接的超时时间；socketTimeout 配置发送请求后等待响应的超时时间。 
> **注意**：不设置这两项超时时间，服务会有高的风险。  
> 例如网络异常下 socket 没有办法检测到网络错误，如果没有设置 socket 网络超时，连接就会一直等待 DB 返回结果，造成新的请求都无法获取到连接。  

3. **maxActive**  
	最大连接池数量，允许的最大同时使用中的连接数。 
>**注意**：当业务出现大流量涌入时，连接池耗尽，maxWait未配置或者配置为 0 时将无限等待，导致等待队列越来越长，表现为业务接口大量超时，实际吞吐越低。
