From centos:centos7
RUN yum install -y wget java mysql

# to https://github.com/actiontech/dble/releases get latest package download locally
COPY dble-3.20.07.0-20200831034733-linux.tar.gz /opt/

RUN tar zxvf opt/dble-3.20.07.0-20200831034733-linux.tar.gz -C /opt && \
        rm -f opt/dble-3.20.07.0-20200831034733-linux.tar.gz

COPY quick-start/*.xml /opt/dble/conf/

COPY quick-start/*.cnf /opt/dble/conf/
COPY quick-start/docker_init_start.sh /opt/dble/bin/
COPY wait-for-it.sh /opt/dble/bin/

RUN chmod 777 /opt/dble/bin/*

VOLUME /opt/dble

CMD ["/opt/dble/bin/docker_init_start.sh"]