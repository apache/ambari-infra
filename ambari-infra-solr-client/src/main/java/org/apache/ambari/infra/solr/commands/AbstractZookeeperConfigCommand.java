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
 * Abstrakcyjna klasa wykonująca polecenia konfiguracji w ZooKeeperze z mechanizmem ponawiania.
 * 
 * Wcześniej wykorzystywano klasy SolrZkClient, SolrZooKeeper i ZkConfigManager, 
 * które zostały usunięte w Solr 9.8.0. Teraz operacje na konfiguracji wykonujemy
 * przy użyciu bezpośrednio uzyskanego obiektu ZooKeeper.
 */
public abstract class AbstractZookeeperConfigCommand<RESPONSE> extends AbstractZookeeperRetryCommand<RESPONSE> {

  public AbstractZookeeperConfigCommand(int maxRetries, int interval) {
    super(maxRetries, interval);
  }

  /**
   * Abstrakcyjna metoda wykonująca operację konfiguracji na ZooKeeperze.
   *
   * @param zk instancja ZooKeeper
   * @param client odniesienie do AmbariSolrCloudClient
   * @return wynik operacji
   * @throws Exception w przypadku błędów
   */
  protected abstract RESPONSE executeZkConfigCommand(ZooKeeper zk, AmbariSolrCloudClient client)
    throws Exception;

  @Override
  protected RESPONSE executeZkCommand(AmbariSolrCloudClient client, ZooKeeper zk) throws Exception {
    return executeZkConfigCommand(zk, client);
  }
}
