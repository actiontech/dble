From centos:centos7
RUN yum install -y wget java mysql

RUN wget -P /opt ftp://ftp:ftp@10.186.18.20/actiontech-dble/qa/9.9.9.9/actiontech-dble.tar.gz && \
    tar zxvf /opt/actiontech-dble.tar.gz -C /opt && \
    wget https://raw.githubusercontent.com/sunsun314/dble/btrace-add/docker-images/conf/schema.xml -P /opt/dble/conf/ && \
    wget https://raw.githubusercontent.com/sunsun314/dble/btrace-add/docker-images/conf/server.xml -P /opt/dble/conf/ && \
    wget https://raw.githubusercontent.com/sunsun314/dble/btrace-add/docker-images/conf/rule.xml -P /opt/dble/conf/ && \
    wget https://raw.githubusercontent.com/sunsun314/dble/btrace-add/docker-images/conf/wrapper.conf -P /opt/dble/conf/ && \
    wget https://raw.githubusercontent.com/sunsun314/dble/btrace-add/docker-images/conf/docker_init_start.sh -P /opt/dble/bin/ && \
    chmod 777 /opt/dble/bin/docker_init_start.sh && \
    rm -f /opt/actiontech-dble.tar.gz

VOLUME /opt/dble

CMD ["/opt/dble/bin/docker_init_start.sh"]