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
package org.apache.ambari.infra.job.archive;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

public class HdfsUploader extends AbstractFileAction {

  private static final String DEFAULT_FILE_PERMISSION = "640";
  private final Configuration configuration;
  private final Path destinationDirectory;
  private final FsPermission fsPermission;

  public HdfsUploader(Configuration configuration, Path destinationDirectory, FsPermission fsPermission) {
    this.destinationDirectory = destinationDirectory;
    this.configuration = configuration;
    this.fsPermission = fsPermission == null ? new FsPermission(DEFAULT_FILE_PERMISSION) : fsPermission;
  }

  @Override
  protected File onPerform(File inputFile) {
    try (FileSystem fileSystem = FileSystem.get(configuration)) {
      Path destination = new Path(destinationDirectory, inputFile.getName());
      if (fileSystem.exists(destination)) {
        throw new UnsupportedOperationException(String.format("File '%s' already exists!", destination));
      }

      fileSystem.copyFromLocalFile(new Path(inputFile.getAbsolutePath()), destination);
      fileSystem.setPermission(destination, fsPermission);

      return inputFile;
    }
    catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
