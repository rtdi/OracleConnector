FROM rtdi/connectorrootapp

LABEL maintainer="info@rtdi.io"

ADD https://raw.githubusercontent.com/rtdi/OracleConnector/master/docker/global.properties conf/rtdiconfig/OracleConnector/
ADD https://github.com/rtdi/OracleConnector/releases/latest/download/oracleconnector.war ./webapps/

EXPOSE 8080
EXPOSE 8443


CMD ["appcontainer.sh"]
