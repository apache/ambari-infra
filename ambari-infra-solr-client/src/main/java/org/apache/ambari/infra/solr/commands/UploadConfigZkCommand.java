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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.ambari.infra.solr.AmbariSolrCloudClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

public class UploadConfigZkCommand extends AbstractZookeeperConfigCommand<String> {

  public UploadConfigZkCommand(int maxRetries, int interval) {
    super(maxRetries, interval);
  }

  @Override
  protected String executeZkConfigCommand(ZooKeeper zk, AmbariSolrCloudClient client) throws Exception {
    Path configDir = Paths.get(client.getConfigDir());
    String configSet = client.getConfigSet();
    String zkBasePath = "/configs/" + configSet;
    uploadDirectory(zk, configDir, zkBasePath);
    return configSet;
  }

  private void uploadDirectory(ZooKeeper zk, Path localDir, String zkPath) throws Exception {
    // Jeśli węzeł jeszcze nie istnieje, utwórz go jako katalog (pusty bajt[] jako dane)
    if (zk.exists(zkPath, false) == null) {
      zk.create(zkPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
    // Dla każdego elementu w lokalnym katalogu:
    Files.list(localDir).forEach(path -> {
      String childZkPath = zkPath + "/" + path.getFileName().toString();
      try {
        if (Files.isDirectory(path)) {
          // Rekurencyjne przetwarzanie katalogu
          uploadDirectory(zk, path, childZkPath);
        } else {
          byte[] content = Files.readAllBytes(path);
          if (zk.exists(childZkPath, false) == null) {
            zk.create(childZkPath, content, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
          } else {
            zk.setData(childZkPath, content, -1);
          }
        }
      } catch (Exception e) {
        throw new RuntimeException("Error uploading file: " + path, e);
      }
    });
  }
}
