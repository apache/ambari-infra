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

import java.util.List;

import org.apache.ambari.infra.solr.AmbariSolrCloudClient;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

public class DeleteZnodeZkCommand extends AbstractZookeeperRetryCommand<Boolean> {

  public DeleteZnodeZkCommand(int maxRetries, int interval) {
    super(maxRetries, interval);
  }

  @Override
  protected Boolean executeZkCommand(AmbariSolrCloudClient client, ZooKeeper zk) throws Exception {
    String path = client.getZnode();
    deleteRecursive(zk, path);
    return true;
  }

  /**
   * Rekurencyjnie usuwa węzeł i wszystkie jego dzieci.
   *
   * @param zk   instancja ZooKeeper
   * @param path ścieżka do węzła, który ma zostać usunięty
   * @throws Exception w przypadku błędu podczas usuwania
   */
  private void deleteRecursive(ZooKeeper zk, String path) throws Exception {
    try {
      List<String> children = zk.getChildren(path, false);
      for (String child : children) {
        String childPath = path.endsWith("/") ? path + child : path + "/" + child;
        deleteRecursive(zk, childPath);
      }
      zk.delete(path, -1);
    } catch (KeeperException.NoNodeException e) {
      // Węzeł już nie istnieje, można bezpiecznie zakończyć
    }
  }
}
