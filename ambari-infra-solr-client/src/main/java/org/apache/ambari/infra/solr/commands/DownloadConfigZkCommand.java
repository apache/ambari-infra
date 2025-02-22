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
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class DownloadConfigZkCommand extends AbstractZookeeperConfigCommand<String> {

  public DownloadConfigZkCommand(int maxRetries, int interval) {
    super(maxRetries, interval);
  }

  @Override
  protected String executeZkConfigCommand(ZooKeeper zk, AmbariSolrCloudClient client) throws Exception {
    Path configDir = Paths.get(client.getConfigDir());
    String configSet = client.getConfigSet();
    String zkBasePath = "/configs/" + configSet;
    try {
      if (!Files.exists(configDir)) {
        Files.createDirectories(configDir);
      }
      downloadRecursively(zk, zkBasePath, configDir);
      return configDir.toString();
    } catch (IOException e) {
      throw new AmbariSolrCloudClientException("Error downloading configuration set, check if Solr Znode has started or not", e);
    }
  }

  /**
   * Pobiera rekurencyjnie zawartość drzewa znodów z ZooKeepera i zapisuje ją do lokalnego katalogu.
   *
   * @param zk       instancja ZooKeeper
   * @param zkPath   bieżąca ścieżka w ZooKeeperze (np. /configs/<configSet>)
   * @param localPath lokalna ścieżka, do której zapisywana będzie zawartość
   * @throws Exception w przypadku błędów podczas pobierania lub zapisu
   */
  private void downloadRecursively(ZooKeeper zk, String zkPath, Path localPath) throws Exception {
    // Pobierz dane dla bieżącego węzła (jeśli istnieją)
    byte[] data = null;
    try {
      data = zk.getData(zkPath, false, null);
    } catch (Exception e) {
      // Jeśli węzeł nie posiada danych lub wystąpi inny błąd, kontynuujemy
    }
    List<String> children = zk.getChildren(zkPath, false);
    if (children.isEmpty()) {
      // Jeśli węzeł jest liściem – zapisz dane do pliku (lub utwórz pusty plik, jeśli brak danych)
      if (data != null) {
        Files.write(localPath, data);
      } else if (!Files.exists(localPath)) {
        Files.createFile(localPath);
      }
    } else {
      // Jeśli węzeł ma dzieci – traktujemy go jako katalog
      if (!Files.exists(localPath)) {
        Files.createDirectories(localPath);
      }
      for (String child : children) {
        String childZkPath = zkPath.endsWith("/") ? zkPath + child : zkPath + "/" + child;
        Path childLocalPath = localPath.resolve(child);
        downloadRecursively(zk, childZkPath, childLocalPath);
      }
    }
  }
}
