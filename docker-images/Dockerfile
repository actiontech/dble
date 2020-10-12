From centos:centos7
RUN yum install -y wget java mysql

RUN version=$(curl "https://github.com/actiontech/dble/releases/latest"|awk -F "/" '{print $8}') && \
    wget -P /opt https://github.com/actiontech/dble/releases/download/$version%2Ftag/actiontech-dble-$version.tar.gz && \
    tar zxvf /opt/actiontech-dble-$version.tar.gz -C /opt && \
    rm -f /opt/actiontech-dble-$version.tar.gz

COPY quick-start/*.xml /opt/dble/conf/

COPY quick-start/*.cnf /opt/dble/conf/
COPY quick-start/docker_init_start.sh /opt/dble/bin/
COPY wait-for-it.sh /opt/dble/bin/

RUN chmod 777 /opt/dble/bin/*

VOLUME /opt/dble

CMD ["/opt/dble/bin/docker_init_start.sh"]