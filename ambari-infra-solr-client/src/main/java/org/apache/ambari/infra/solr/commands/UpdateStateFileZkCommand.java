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

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.ambari.infra.solr.AmbariSolrCloudClient;
import org.apache.ambari.infra.solr.domain.AmbariSolrState;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateStateFileZkCommand extends AbstractStateFileZkCommand {

  private static final Logger logger = LoggerFactory.getLogger(UpdateStateFileZkCommand.class);

  private String unsecureZnode;

  public UpdateStateFileZkCommand(int maxRetries, int interval, String unsecureZnode) {
    super(maxRetries, interval);
    this.unsecureZnode = unsecureZnode;
  }

  @Override
  protected AmbariSolrState executeZkCommand(AmbariSolrCloudClient client, ZooKeeper zk) throws Exception {
    boolean secure = client.isSecure();
    String stateFile = String.format("%s/%s", unsecureZnode, AbstractStateFileZkCommand.STATE_FILE);
    AmbariSolrState result;
    if (secure) {
      logger.info("Update state file in secure mode.");
      updateStateFile(client, zk, AmbariSolrState.SECURE, stateFile);
      result = AmbariSolrState.SECURE;
    } else {
      logger.info("Update state file in unsecure mode.");
      updateStateFile(client, zk, AmbariSolrState.UNSECURE, stateFile);
      result = AmbariSolrState.UNSECURE;
    }
    return result;
  }

  private void updateStateFile(AmbariSolrCloudClient client, ZooKeeper zk, AmbariSolrState stateToUpdate,
                               String stateFile) throws Exception {
    if (zk.exists(stateFile, false) == null) {
      logger.info("State file does not exist. Initializing it as '{}'", stateToUpdate);
      zk.create(stateFile, createStateJson(stateToUpdate).getBytes(StandardCharsets.UTF_8),
          ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    } else {
      // Przekazujemy obiekt ZooKeeper zamiast client
      AmbariSolrState stateOnSecure = getStateFromJson(zk, stateFile);
      if (stateToUpdate.equals(stateOnSecure)) {
        logger.info("State file is in '{}' mode. No update.", stateOnSecure);
      } else {
        logger.info("State file is in '{}' mode. Updating it to '{}'", stateOnSecure, stateToUpdate);
        zk.setData(stateFile, createStateJson(stateToUpdate).getBytes(StandardCharsets.UTF_8), -1);
      }
    }
  }

  private String createStateJson(AmbariSolrState state) throws Exception {
    Map<String, String> secStateMap = new HashMap<>();
    secStateMap.put(AbstractStateFileZkCommand.STATE_FIELD, state.toString());
    return new ObjectMapper().writeValueAsString(secStateMap);
  }
}
