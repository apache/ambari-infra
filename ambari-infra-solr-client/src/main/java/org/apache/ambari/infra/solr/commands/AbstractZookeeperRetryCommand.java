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
import org.apache.zookeeper.ZooKeeper;

/**
 * Abstrakcyjna klasa wykonująca polecenia na ZooKeeperze z mechanizmem ponawiania.
 * 
 * Wcześniej wykorzystywano klasy SolrZkClient i SolrZooKeeper, które zostały
 * usunięte w Solr 9.8.0. Teraz tworzymy połączenie do ZooKeepera bezpośrednio,
 * korzystając z łańcucha połączeniowego dostępnego w AmbariSolrCloudClient.
 */
public abstract class AbstractZookeeperRetryCommand<RESPONSE> extends AbstractRetryCommand<RESPONSE> {

  public AbstractZookeeperRetryCommand(int maxRetries, int interval) {
    super(maxRetries, interval);
  }

  /**
   * Metoda abstrakcyjna do wykonania operacji na ZooKeeperze.
   *
   * @param client odniesienie do AmbariSolrCloudClient
   * @param zk instancja ZooKeeper utworzona na podstawie zkConnectString
   * @return wynik operacji
   * @throws Exception w przypadku błędów
   */
  protected abstract RESPONSE executeZkCommand(AmbariSolrCloudClient client, ZooKeeper zk)
    throws Exception;

  @Override
  public RESPONSE createAndProcessRequest(AmbariSolrCloudClient client) throws Exception {
    // Używamy zkConnectString z klienta do utworzenia połączenia do ZooKeepera.
    int sessionTimeout = 30000; // Timeout sesji (w ms) – można uczynić konfigurowalnym.
    ZooKeeper zk = new ZooKeeper(client.getZkConnectString(), sessionTimeout, event -> {
      // Można tutaj dodać obsługę zdarzeń ZooKeepera, jeśli jest potrzebna.
    });
    return executeZkCommand(client, zk);
  }
}
