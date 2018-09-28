#!/usr/bin/env bash
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

readlinkf(){
  # get real path on mac OSX
  perl -MCwd -e 'print Cwd::abs_path shift' "$1";
}

if [ "$(uname -s)" = 'Linux' ]; then
  SCRIPT_DIR="`dirname "$(readlink -f "$0")"`"
else
  SCRIPT_DIR="`dirname "$(readlinkf "$0")"`"
fi

INFRA_PROJECT_DIR="`dirname \"$SCRIPT_DIR\"`"

cd $INFRA_PROJECT_DIR

mvn clean package -DskipTests -Drat.skip=true -Dbuild-deb

docker build -t apache/ambari-infra-solr:latest -f jenkins/containers/infra-solr/Dockerfile .
docker push apache/ambari-infra-solr:latest
