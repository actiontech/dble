From centos:centos7
RUN yum install -y wget java mysql

RUN wget -P /opt https://github.com/actiontech/dble/releases/download/2.18.12.0%2Ftag/actiontech-dble-2.18.12.0.tar.gz && \
    tar zxvf /opt/actiontech-dble-2.18.12.0.tar.gz -C /opt && \
    wget https://raw.githubusercontent.com/actiontech/dble/docker-file/docker-images/conf/schema.xml -P /opt/dble/conf/ && \
    wget https://raw.githubusercontent.com/actiontech/dble/docker-file/docker-images/conf/server.xml -P /opt/dble/conf/ && \
    wget https://raw.githubusercontent.com/actiontech/dble/docker-file/docker-images/conf/rule.xml -P /opt/dble/conf/ && \
    wget https://raw.githubusercontent.com/actiontech/dble/docker-file/docker-images/conf/wrapper.conf -P /opt/dble/conf/ && \
    wget https://raw.githubusercontent.com/actiontech/dble/docker-file/docker-images/conf/docker_init_start.sh -P /opt/dble/bin/ && \
    chmod 777 /opt/dble/bin/docker_init_start.sh && \
    rm -f /opt/actiontech-dble-2.18.12.0.tar.gz

VOLUME /opt/dble

CMD ["/opt/dble/bin/docker_init_start.sh"]