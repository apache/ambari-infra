/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.ambari.infra.solr.commands;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ambari.infra.solr.AmbariSolrCloudClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.HashMap;
import java.util.Map;

public class SetClusterPropertyZkCommand extends AbstractZookeeperRetryCommand<String> {

  public SetClusterPropertyZkCommand(int maxRetries, int interval) {
    super(maxRetries, interval);
  }

  @Override
  protected String executeZkCommand(AmbariSolrCloudClient client, ZooKeeper zk) throws Exception {
    String propertyName = client.getPropName();
    String propertyValue = client.getPropValue();
    String clusterPropsPath = client.getZnode() + "/clusterprops.json";

    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> clusterProps;
    Stat stat = zk.exists(clusterPropsPath, false);
    if (stat != null) {
      byte[] data = zk.getData(clusterPropsPath, false, null);
      clusterProps = mapper.readValue(data, new TypeReference<Map<String, Object>>() {});
    } else {
      clusterProps = new HashMap<>();
    }

    // Ustawiamy właściwość
    clusterProps.put(propertyName, propertyValue);
    byte[] newData = mapper.writeValueAsBytes(clusterProps);

    if (stat != null) {
      zk.setData(clusterPropsPath, newData, stat.getVersion());
    } else {
      zk.create(clusterPropsPath, newData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
    return propertyValue;
  }
}
