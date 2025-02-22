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
package org.apache.ambari.infra.solr.domain;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.zookeeper.CreateMode.PERSISTENT;

import java.util.Optional;

import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

public class ZookeeperClient {
  private final ZooKeeper zk;

  public ZookeeperClient(ZooKeeper zk) {
    this.zk = zk;
  }

  public void putFileContent(String fileName, String content) throws Exception {
    if (zk.exists(fileName, false) != null) {
      zk.setData(fileName, content.getBytes(UTF_8), -1);
    } else {
      zk.create(fileName, content.getBytes(UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, PERSISTENT);
    }
  }

  public Optional<String> getFileContent(String fileName) throws Exception {
    if (zk.exists(fileName, false) == null)
      return Optional.empty();
    byte[] data = zk.getData(fileName, false, null);
    return Optional.of(new String(data, UTF_8));
  }
}
