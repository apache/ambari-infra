/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership. The ASF
 * licenses this file under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
 * OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.ambari.infra.solr.commands;

import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.ambari.infra.solr.AmbariSolrCloudClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

public class TransferZnodeZkCommand extends AbstractZookeeperRetryCommand<Boolean> {

  public TransferZnodeZkCommand(int maxRetries, int interval) {
    super(maxRetries, interval);
  }

  @Override
  protected Boolean executeZkCommand(AmbariSolrCloudClient client, ZooKeeper zk) throws Exception {
    String transferMode = client.getTransferMode();
    String copySrc = client.getCopySrc();
    String copyDest = client.getCopyDest();

    if ("copyToLocal".equals(transferMode)) {
      // Kopiowanie z ZooKeepera do lokalnego systemu plików
      byte[] data = zk.getData(copySrc, false, null);
      Files.write(Paths.get(copyDest), data);
    } else if ("copyFromLocal".equals(transferMode)) {
      // Kopiowanie z lokalnego systemu plików do ZooKeepera
      byte[] data = Files.readAllBytes(Paths.get(copySrc));
      if (zk.exists(copyDest, false) != null) {
        zk.setData(copyDest, data, -1);
      } else {
        // Zakładamy, że rodzicielski węzeł istnieje – w razie potrzeby można dodać rekurencyjne tworzenie
        zk.create(copyDest, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
    } else {
      // Domyślny tryb: kopiowanie pomiędzy węzłami w ZooKeeperze
      byte[] data = zk.getData(copySrc, false, null);
      if (zk.exists(copyDest, false) != null) {
        zk.setData(copyDest, data, -1);
      } else {
        zk.create(copyDest, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
    }
    return true;
  }
}
