/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.ambari.infra.solr;

import java.util.Collection;
import java.util.List;

import org.apache.ambari.infra.solr.commands.CheckConfigZkCommand;
import org.apache.ambari.infra.solr.commands.CheckZnodeZkCommand;
import org.apache.ambari.infra.solr.commands.CreateCollectionCommand;
import org.apache.ambari.infra.solr.commands.CreateShardCommand;
import org.apache.ambari.infra.solr.commands.CreateSolrZnodeZkCommand;
import org.apache.ambari.infra.solr.commands.DeleteZnodeZkCommand;
import org.apache.ambari.infra.solr.commands.DownloadConfigZkCommand;
import org.apache.ambari.infra.solr.commands.DumpCollectionsCommand;
import org.apache.ambari.infra.solr.commands.EnableKerberosPluginSolrZkCommand;
import org.apache.ambari.infra.solr.commands.GetShardsCommand;
import org.apache.ambari.infra.solr.commands.GetSolrHostsCommand;
import org.apache.ambari.infra.solr.commands.ListCollectionCommand;
import org.apache.ambari.infra.solr.commands.RemoveAdminHandlersCommand;
import org.apache.ambari.infra.solr.commands.SecureSolrZNodeZkCommand;
import org.apache.ambari.infra.solr.commands.SecureZNodeZkCommand;
import org.apache.ambari.infra.solr.commands.SetAutoScalingZkCommand;
import org.apache.ambari.infra.solr.commands.SetClusterPropertyZkCommand;
import org.apache.ambari.infra.solr.commands.TransferZnodeZkCommand;
import org.apache.ambari.infra.solr.commands.UnsecureZNodeZkCommand;
import org.apache.ambari.infra.solr.commands.UploadConfigZkCommand;
import org.apache.ambari.infra.solr.util.ShardUtils;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.cloud.Slice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client do komunikacji z Solr (oraz ZooKeeper).
 */
public class AmbariSolrCloudClient {

  private static final Logger logger = LoggerFactory.getLogger(AmbariSolrCloudClient.class);

  private final String zkConnectString;
  private final String collection;
  private final String configSet;
  private final String configDir;
  private final int shards;
  private final int replication;
  private final int retryTimes;
  private final int interval;
  private final CloudSolrClient solrCloudClient;
  // Usunięto pole solrZkClient, ponieważ SolrZkClient nie jest już dostępny w Solr 9.8.0
  private final int maxShardsPerNode;
  private final String routerName;
  private final String routerField;
  private final boolean implicitRouting;
  private final String jaasFile;
  private final String znode;
  private final String saslUsers;
  private final String propName;
  private final String propValue;
  private final String securityJsonLocation;
  private final boolean secure;
  private final String transferMode;
  private final String copySrc;
  private final String copyDest;
  private final String output;
  private final boolean includeDocNumber;
  private final String autoScalingJsonLocation;

  public AmbariSolrCloudClient(AmbariSolrCloudClientBuilder builder) {
    this.zkConnectString = builder.zkConnectString;
    this.collection = builder.collection;
    this.configSet = builder.configSet;
    this.configDir = builder.configDir;
    this.shards = builder.shards;
    this.replication = builder.replication;
    this.retryTimes = builder.retryTimes;
    this.interval = builder.interval;
    this.jaasFile = builder.jaasFile;
    this.solrCloudClient = builder.solrCloudClient;
    // Usunięto inicjalizację solrZkClient
    this.maxShardsPerNode = builder.maxShardsPerNode;
    this.routerName = builder.routerName;
    this.routerField = builder.routerField;
    this.implicitRouting = builder.implicitRouting;
    this.znode = builder.znode;
    this.saslUsers = builder.saslUsers;
    this.propName = builder.propName;
    this.propValue = builder.propValue;
    this.securityJsonLocation = builder.securityJsonLocation;
    this.secure = builder.secure;
    this.transferMode = builder.transferMode;
    this.copySrc = builder.copySrc;
    this.copyDest = builder.copyDest;
    this.output = builder.output;
    this.includeDocNumber = builder.includeDocNumber;
    this.autoScalingJsonLocation = builder.autoScalingJsonLocation;
  }

  /**
   * Pobierz listę kolekcji Solr.
   */
  public List<String> listCollections() throws Exception {
    return new ListCollectionCommand(getRetryTimes(), getInterval()).run(this);
  }

  /**
   * Utwórz kolekcję Solr, jeśli nie istnieje.
   */
  public String createCollection() throws Exception {
    List<String> collections = listCollections();
    if (!collections.contains(getCollection())) {
      String collection = new CreateCollectionCommand(getRetryTimes(), getInterval()).run(this);
      logger.info("Wysłano żądanie utworzenia kolekcji '{}'.", collection);
    } else {
      logger.info("Kolekcja '{}' już istnieje.", getCollection());
      if (this.isImplicitRouting()) {
        createShard(null);
      }
    }
    return getCollection();
  }

  /**
   * Wypisz dane kolekcji.
   */
  public String outputCollectionData() throws Exception {
    List<String> collections = listCollections();
    String result = new DumpCollectionsCommand(getRetryTimes(), getInterval(), collections).run(this);
    logger.info("Odpowiedź dump kolekcji: {}", result);
    return result;
  }

  /**
   * Ustaw właściwość klastra w clusterprops.json.
   */
  public void setClusterProp() throws Exception {
    logger.info("Ustaw właściwość klastra: '{}'", this.getPropName());
    String newPropValue = new SetClusterPropertyZkCommand(getRetryTimes(), getInterval()).run(this);
    logger.info("Właściwość klastra '{}' została ustawiona na '{}'", this.getPropName(), newPropValue);
  }

  /**
   * Utwórz znode, jeśli jeszcze nie istnieje. Zwraca 0, jeśli już istnieje.
   */
  public void createZnode() throws Exception {
    boolean znodeExists = isZnodeExists(this.znode);
    if (znodeExists) {
      logger.info("Znode '{}' już istnieje.", this.znode);
    } else {
      logger.info("Znode '{}' nie istnieje. Tworzenie...", this.znode);
      String newZnode = new CreateSolrZnodeZkCommand(getRetryTimes(), getInterval()).run(this);
      logger.info("Znode '{}' został utworzony pomyślnie.", newZnode);
    }
  }

  /**
   * Sprawdź, czy znode istnieje.
   */
  public boolean isZnodeExists(String znode) throws Exception {
    logger.info("Sprawdzenie, czy znode '{}' istnieje.", znode);
    boolean result = new CheckZnodeZkCommand(getRetryTimes(), getInterval(), znode).run(this);
    if (result) {
      logger.info("Znode '{}' istnieje.", znode);
    } else {
      logger.info("Znode '{}' nie istnieje.", znode);
    }
    return result;
  }

  /**
   * Skonfiguruj wtyczkę Kerberos w security.json.
   */
  public void setupKerberosPlugin() throws Exception {
    logger.info("Konfiguracja wtyczki Kerberos w security.json");
    new EnableKerberosPluginSolrZkCommand(getRetryTimes(), getInterval()).run(this);
    logger.info("Wtyczka Kerberos została ustawiona w security.json");
  }

  /**
   * Zabezpiecz znode Solr.
   */
  public void secureSolrZnode() throws Exception {
    new SecureSolrZNodeZkCommand(getRetryTimes(), getInterval()).run(this);
  }

  /**
   * Zabezpiecz znode.
   */
  public void secureZnode() throws Exception {
    new SecureZNodeZkCommand(getRetryTimes(), getInterval()).run(this);
  }

  /**
   * Wyłącz zabezpieczenia dla znode.
   */
  public void unsecureZnode() throws Exception {
    logger.info("Wyłączanie zabezpieczeń dla znode - {}", this.getZnode());
    new UnsecureZNodeZkCommand(getRetryTimes(), getInterval()).run(this);
  }

  /**
   * Prześlij konfigurację do ZooKeepera.
   */
  public String uploadConfiguration() throws Exception {
    String configSet = new UploadConfigZkCommand(getRetryTimes(), getInterval()).run(this);
    logger.info("Konfiguracja '{}' została przesłana do ZooKeepera.", configSet);
    return configSet;
  }

  /**
   * Pobierz konfigurację z ZooKeepera.
   */
  public String downloadConfiguration() throws Exception {
    String configDir = new DownloadConfigZkCommand(getRetryTimes(), getInterval()).run(this);
    logger.info("Konfiguracja została pobrana z ZooKeepera. ({})", configDir);
    return configDir;
  }

  /**
   * Sprawdź, czy konfiguracja istnieje w ZooKeeperze.
   */
  public boolean configurationExists() throws Exception {
    boolean configExits = new CheckConfigZkCommand(getRetryTimes(), getInterval()).run(this);
    if (configExits) {
      logger.info("Konfiguracja {} istnieje", configSet);
    } else {
      logger.info("Konfiguracja '{}' nie istnieje", configSet);
    }
    return configExits;
  }

  /**
   * Utwórz shard w kolekcji. Jeśli nazwa sharda jest podana, utworzy ten shard, 
   * w przeciwnym razie utworzy shardy według logiki (nazwa shard_#).
   */
  public Collection<String> createShard(String shard) throws Exception {
    Collection<String> existingShards = getShardNames();
    if (shard != null) {
      new CreateShardCommand(shard, getRetryTimes(), getInterval()).run(this);
      existingShards.add(shard);
    } else {
      List<String> shardList = ShardUtils.generateShardList(getMaxShardsPerNode());
      for (String shardName : shardList) {
        if (!existingShards.contains(shardName)) {
          new CreateShardCommand(shardName, getRetryTimes(), getInterval()).run(this);
          logger.info("Wysłano żądanie dodania nowego sharda ('{}': {})", getCollection(), shardName);
          existingShards.add(shardName);
        }
      }
    }
    return existingShards;
  }

  /**
   * Pobierz nazwy shardów.
   */
  public Collection<String> getShardNames() throws Exception {
    Collection<Slice> slices = new GetShardsCommand(getRetryTimes(), getInterval()).run(this);
    return ShardUtils.getShardNamesFromSlices(slices, this.getCollection());
  }

  /**
   * Pobierz hosty Solr.
   */
  public Collection<String> getSolrHosts() throws Exception {
    return new GetSolrHostsCommand(getRetryTimes(), getInterval()).run(this);
  }

  /**
   * Usuń requestHandler solr.admin.AdminHandlers z pliku solrconfig.xml.
   */
  public boolean removeAdminHandlerFromCollectionConfig() throws Exception {
    return new RemoveAdminHandlersCommand(getRetryTimes(), getInterval()).run(this);
  }

  /**
   * Przenieś dane znode (nie można mieć jednocześnie źródła i celu lokalnego).
   */
  public boolean transferZnode() throws Exception {
    return new TransferZnodeZkCommand(getRetryTimes(), getInterval()).run(this);
  }

  /**
   * Usuń ścieżkę znode (wraz z podwęzłami).
   */
  public boolean deleteZnode() throws Exception {
    return new DeleteZnodeZkCommand(getRetryTimes(), getInterval()).run(this);
  }

  /**
   * Ustaw auto-scaling.
   */
  public void setAutoScaling() throws Exception {
    new SetAutoScalingZkCommand(getRetryTimes(), getInterval(), autoScalingJsonLocation).run(this);
  }

  public String getZkConnectString() {
    return zkConnectString;
  }

  public String getCollection() {
    return collection;
  }

  public String getConfigSet() {
    return configSet;
  }

  public String getConfigDir() {
    return configDir;
  }

  public int getShards() {
    return shards;
  }

  public int getReplication() {
    return replication;
  }

  public int getRetryTimes() {
    return retryTimes;
  }

  public int getInterval() {
    return interval;
  }

  public CloudSolrClient getSolrCloudClient() {
    return solrCloudClient;
  }

  // Metoda getSolrZkClient została usunięta, gdyż SolrZkClient nie jest już dostępny.

  public int getMaxShardsPerNode() {
    return maxShardsPerNode;
  }

  public String getRouterName() {
    return routerName;
  }

  public String getRouterField() {
    return routerField;
  }

  public boolean isImplicitRouting() {
    return implicitRouting;
  }

  public String getJaasFile() {
    return jaasFile;
  }

  public String getSaslUsers() {
    return saslUsers;
  }

  public String getZnode() {
    return znode;
  }

  public String getPropName() {
    return propName;
  }

  public String getPropValue() {
    return propValue;
  }

  public boolean isSecure() {
    return secure;
  }

  public String getSecurityJsonLocation() {
    return securityJsonLocation;
  }

  public String getTransferMode() {
    return transferMode;
  }

  public String getCopySrc() {
    return copySrc;
  }

  public String getCopyDest() {
    return copyDest;
  }

  public String getOutput() {
    return output;
  }

  public boolean isIncludeDocNumber() {
    return includeDocNumber;
  }
}
