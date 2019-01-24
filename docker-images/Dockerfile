From centos:centos7
RUN yum install -y wget java mysql

RUN version=$(curl "https://github.com/actiontech/dble/releases/latest"|awk -F "/" '{print $8}') && \
    wget -P /opt https://github.com/actiontech/dble/releases/download/$version%2Ftag/actiontech-dble-$version.tar.gz && \
    tar zxvf /opt/actiontech-dble-$version.tar.gz -C /opt && \
    wget https://raw.githubusercontent.com/actiontech/dble/master/docker-images/quick-start/schema.xml -P /opt/dble/conf/ && \
    wget https://raw.githubusercontent.com/actiontech/dble/master/docker-images/quick-start/server.xml -P /opt/dble/conf/ && \
    wget https://raw.githubusercontent.com/actiontech/dble/master/docker-images/quick-start/rule.xml -P /opt/dble/conf/ && \
    wget https://raw.githubusercontent.com/actiontech/dble/master/docker-images/quick-start/wrapper.conf -P /opt/dble/conf/ && \
    wget https://raw.githubusercontent.com/actiontech/dble/master/docker-images/quick-start/docker_init_start.sh -P /opt/dble/bin/ && \
    wget https://raw.githubusercontent.com/actiontech/dble/master/docker-images/wait-for-it.sh -P /opt/dble/bin/ && \
    chmod 777 /opt/dble/bin/* && \
    rm -f /opt/actiontech-dble-$version.tar.gz

VOLUME /opt/dble

CMD ["/opt/dble/bin/docker_init_start.sh"]