From centos:centos7
RUN yum install -y wget java mysql python3 python3-devel pip3 mysql-devel gcc

RUN version=$(curl "https://github.com/actiontech/dble/releases/latest"|awk -F "/" '{print $8}') && \
    wget -P /opt https://github.com/actiontech/dble/releases/download/$version%2Ftag/actiontech-dble-$version.tar.gz && \
    tar zxvf /opt/actiontech-dble-$version.tar.gz -C /opt && \
    wget https://github.com/actiontech/dble/blob/$version/tag/docker-images/quick-start/schema.xml -P /opt/dble/conf/ && \
    wget https://github.com/actiontech/dble/blob/$version/tag/docker-images/quick-start/server.xml -P /opt/dble/conf/ && \
    wget https://github.com/actiontech/dble/blob/$version/tag/docker-images/quick-start/rule.xml -P /opt/dble/conf/ && \
    wget https://github.com/actiontech/dble/blob/$version/tag/docker-images/quick-start/wrapper.conf -P /opt/dble/conf/ && \
    wget https://github.com/actiontech/dble/blob/$version/tag/docker-images/quick-start/docker_init_start.sh -P /opt/dble/bin/ && \
    wget https://github.com/actiontech/dble/blob/$version/tag/docker-images/wait-for-it.sh -P /opt/dble/bin/ && \
    rm -f /opt/actiontech-dble-$version.tar.gz && \
    pip3 install mysqlclient -i http://pypi.douban.com/simple --trusted-host pypi.douban.com && \
    pip3 install six -i http://pypi.douban.com/simple --trusted-host pypi.douban.com && \
    pip3 install coloredlogs -i http://pypi.douban.com/simple --trusted-host pypi.douban.com && \
    pip3 install rsa -i http://pypi.douban.com/simple --trusted-host pypi.douban.com && \
    chmod 777 /opt/dble/bin/* && \
    chmod +x /opt/dble/bin/*.py

VOLUME /opt/dble

CMD ["/opt/dble/bin/docker_init_start.sh"]
