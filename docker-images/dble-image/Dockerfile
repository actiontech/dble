From centos:centos7
RUN yum install -y wget java mysql python3 python3-devel pip3 mysql-devel gcc && \
    yum clean all && rm -rf /var/cache/yum/*&& \
    pip3 install mysqlclient==1.3.12 -i http://pypi.douban.com/simple --trusted-host pypi.douban.com && \
    pip3 install six -i http://pypi.douban.com/simple --trusted-host pypi.douban.com && \
    pip3 install coloredlogs -i http://pypi.douban.com/simple --trusted-host pypi.douban.com && \
    pip3 install rsa -i http://pypi.douban.com/simple --trusted-host pypi.douban.com

COPY docker_init_start.sh /opt/dble/bin/
COPY wait-for-it.sh /opt/dble/bin/

ARG MODE="quick-start"

RUN if [ "$MODE" != "mgr" -a "$MODE" != "quick-start" -a "$MODE" != "rwSplit" ]; then MODE="quick-start"; fi;

COPY *.cnf /opt/dble/conf/
COPY "$MODE"/* /opt/dble/conf/

ARG DBLE_VERSION="latest"

RUN if [[ "$DBLE_VERSION" != "latest" ]]; then DBLE_VERSION="tags/${DBLE_VERSION}/tag"; fi;

RUN wget -P /opt $(curl https://api.github.com/repos/actiontech/dble/releases/${DBLE_VERSION} | grep "browser_download_url.*tar.gz" | cut -d '"' -f 4) && \
    tar zxf /opt/dble-*.tar.gz -C /opt && \
    rm -f /opt/dble-*.tar.gz

RUN chmod 777 /opt/dble/bin/*

VOLUME /opt/dble/

EXPOSE 8066 9066

CMD ["/opt/dble/bin/docker_init_start.sh"]
