#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
FROM openjdk:10-jre
MAINTAINER dev@ambari.apache.org

RUN apt-get update && apt-get install -y python
ADD jenkins/containers/infra-solr/bin /infra-solr/bin
ADD jenkins/containers/infra-solr/conf /infra-solr/conf
ADD ambari-infra-assembly/target/ambari-infra-solr_2.0.0.0-SNAPSHOT.deb /infra-solr/ambari-infra-solr.deb
ADD ambari-infra-assembly/target/ambari-infra-solr-client_2.0.0.0-SNAPSHOT.deb /infra-solr/ambari-infra-solr-client.deb
RUN dpkg -i /infra-solr/ambari-infra-solr.deb
RUN dpkg -i /infra-solr/ambari-infra-solr-client.deb
RUN mkdir -p /var/lib/ambari-infra-solr/data

ENV SOLR_USER="infra-solr"
ENV SOLR_GROUP="infra-solr"
ENV SOLR_UID="8983"
ENV SOLR_GID="8983"
ENV SOLR_HOME /var/lib/ambari-infra-solr/data
ENV SOLR_INCLUDE /usr/lib/ambari-infra-solr/bin/solr.in.sh
ENV SOLR_INIT_FILE /infra-solr/bin/init.sh
ENV SOLR_ZNODE /infra-solr
ENV SOLR_PORT="8886"

RUN cp /infra-solr/conf/infra-solr.conf /etc/security/limits.d/

RUN groupadd -r --gid $SOLR_GID $SOLR_GROUP && useradd -r --uid $SOLR_UID --gid $SOLR_GID $SOLR_USER
RUN chown -R $SOLR_USER:$SOLR_GROUP /usr/lib/ambari-infra-solr
RUN chown -R $SOLR_USER:$SOLR_GROUP /var/lib/ambari-infra-solr/data
RUN chown -R $SOLR_USER:$SOLR_GROUP /usr/lib/ambari-infra-solr-client
RUN chown -R $SOLR_USER:$SOLR_GROUP /infra-solr && chmod +x /infra-solr/bin/entrypoint.sh
USER $SOLR_USER

WORKDIR /infra-solr/bin/
ENTRYPOINT ["/infra-solr/bin/entrypoint.sh"]
CMD ["server"]