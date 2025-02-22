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

import org.apache.ambari.infra.solr.AmbariSolrCloudClient;
import org.apache.ambari.infra.solr.AmbariSolrCloudClientException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

public class CheckZnodeZkCommand extends AbstractZookeeperRetryCommand<Boolean> {

  private String znode;

  public CheckZnodeZkCommand(int maxRetries, int interval, String znode) {
    super(maxRetries, interval);
    this.znode = znode;
  }

  @Override
  protected Boolean executeZkCommand(AmbariSolrCloudClient client, ZooKeeper zk) throws Exception {
    try {
      return zk.exists(this.znode, false) != null;
    } catch (KeeperException e) {
      throw new AmbariSolrCloudClientException("Exception during checking znode, " +
        "check if ZooKeeper servers are running or if the quorum has been established.", e);
    }
  }
}
