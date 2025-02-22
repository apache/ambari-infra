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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.ambari.infra.solr.AmbariSolrCloudClient;
import org.apache.ambari.infra.solr.util.AclUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SecureSolrZNodeZkCommand extends AbstractZookeeperRetryCommand<Boolean> {

  private static final Logger logger = LoggerFactory.getLogger(SecureSolrZNodeZkCommand.class);

  public SecureSolrZNodeZkCommand(int maxRetries, int interval) {
    super(maxRetries, interval);
  }

  @Override
  protected Boolean executeZkCommand(AmbariSolrCloudClient client, ZooKeeper zk) throws Exception {
    String zNode = client.getZnode();
    List<ACL> saslUserList = AclUtils.createAclListFromSaslUsers(client.getSaslUsers().split(","));
    List<ACL> newAclList = new ArrayList<>(saslUserList);
    newAclList.add(new ACL(ZooDefs.Perms.READ, new Id("world", "anyone")));

    String configsPath = String.format("%s/%s", zNode, "configs");
    String collectionsPath = String.format("%s/%s", zNode, "collections");
    String aliasesPath = String.format("%s/%s", zNode, "aliases.json"); // TODO: protect this later somehow
    List<String> excludePaths = Arrays.asList(configsPath, collectionsPath, aliasesPath);

    createZnodeIfNeeded(configsPath, zk);
    createZnodeIfNeeded(collectionsPath, zk);

    AclUtils.setRecursivelyOn(zk, zNode, newAclList, excludePaths);

    List<ACL> commonConfigAcls = new ArrayList<>(saslUserList);
    commonConfigAcls.add(new ACL(ZooDefs.Perms.READ | ZooDefs.Perms.CREATE, new Id("world", "anyone")));

    logger.info("Set sasl users for znode '{}' : {}", zNode, StringUtils.join(saslUserList, ","));
    logger.info("Skip {}/configs and {}/collections", zNode, zNode);
    Stat statConfigs = new Stat();
    Stat statCollections = new Stat();
    List<ACL> configsAcls = zk.getACL(configsPath, statConfigs);
    List<ACL> collectionsAcls = zk.getACL(collectionsPath, statCollections);
    zk.setACL(configsPath, AclUtils.mergeAcls(configsAcls, commonConfigAcls), statConfigs.getVersion());
    zk.setACL(collectionsPath, AclUtils.mergeAcls(collectionsAcls, commonConfigAcls), statCollections.getVersion());

    logger.info("Set world:anyone to 'cr' on  {}/configs and {}/collections", zNode, zNode);
    AclUtils.setRecursivelyOn(zk, configsPath, saslUserList);
    AclUtils.setRecursivelyOn(zk, collectionsPath, saslUserList);

    return true;
  }

  private void createZnodeIfNeeded(String path, ZooKeeper zk) throws KeeperException, InterruptedException {
    if (zk.exists(path, false) == null) {
      logger.info("'{}' does not exist. Creating it ...", path);
      // Metoda createPath powinna rekurencyjnie utworzyć całą ścieżkę
      AclUtils.createPath(zk, path);
    }
  }
}
