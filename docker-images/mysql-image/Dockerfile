ARG MYSQL_VERSION=5.7.29

#基础镜像
FROM mysql:${MYSQL_VERSION}

#删除原有的配置文件
RUN rm /etc/mysql/my.cnf

#复制新的配置文件
COPY ./my.cnf /etc/mysql/

#给shell文件赋读权限
RUN chmod a+r /etc/mysql/my.cnf

#创建存放外部配置文件的目录
RUN mkdir /etc/mysql/extend.conf.d

ENTRYPOINT ["docker-entrypoint.sh"]

EXPOSE 3306

CMD ["mysqld"]