From centos:centos7
RUN yum install -y wget java mysql

RUN version=$(curl https://api.github.com/repos/actiontech/dble/releases/latest | grep "name.*tar.gz" | cut -d '"' -f 4) && \
    wget -P /opt $(curl https://api.github.com/repos/actiontech/dble/releases/latest | grep "browser_download_url.*tar.gz" | cut -d '"' -f 4) && \
    tar zxvf /opt/$version -C /opt && \
    rm -f /opt/$version

COPY quick-start/*.xml /opt/dble/conf/

COPY quick-start/*.cnf /opt/dble/conf/
COPY quick-start/docker_init_start.sh /opt/dble/bin/
COPY wait-for-it.sh /opt/dble/bin/

RUN chmod 777 /opt/dble/bin/*

VOLUME /opt/dble

CMD ["/opt/dble/bin/docker_init_start.sh"]