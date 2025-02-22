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
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class EnableKerberosPluginSolrZkCommand extends AbstractZookeeperRetryCommand<String> {

  private static final String SECURITY_JSON = "/security.json";
  private static final String UNSECURE_CONTENT = "{}";

  public EnableKerberosPluginSolrZkCommand(int maxRetries, int interval) {
    super(maxRetries, interval);
  }

  @Override
  protected String executeZkCommand(AmbariSolrCloudClient client, ZooKeeper zk) throws Exception {
    String result = "";
    String filePath = client.getZnode() + SECURITY_JSON;
    String fileContent = getFileContentFromZnode(zk, filePath);
    String securityContent = getFileContent(client.getSecurityJsonLocation());
    if (client.isSecure()) {
      if (!fileContent.equals(securityContent)) {
        putFileContent(zk, filePath, securityContent);
      }
      result = securityContent;
    } else {
      if (!fileContent.equals(UNSECURE_CONTENT)) {
        putFileContent(zk, filePath, UNSECURE_CONTENT);
      }
      result = UNSECURE_CONTENT;
    }
    return result;
  }

  private void putFileContent(ZooKeeper zk, String fileName, String content) throws Exception {
    if (zk.exists(fileName, false) != null) {
      zk.setData(fileName, content.getBytes(StandardCharsets.UTF_8), -1);
    } else {
      zk.create(fileName, content.getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
  }

  private String getFileContentFromZnode(ZooKeeper zk, String fileName) throws Exception {
    String result;
    if (zk.exists(fileName, false) != null) {
      byte[] data = zk.getData(fileName, false, null);
      result = new String(data, StandardCharsets.UTF_8);
    } else {
      result = UNSECURE_CONTENT;
    }
    return result;
  }

  private String getFileContent(String fileLocation) throws IOException {
    File securityJson = new File(fileLocation);
    if (StringUtils.isNotEmpty(fileLocation) && securityJson.exists()) {
      return FileUtils.readFileToString(securityJson, StandardCharsets.UTF_8);
    } else {
      return UNSECURE_CONTENT;
    }
  }
}
