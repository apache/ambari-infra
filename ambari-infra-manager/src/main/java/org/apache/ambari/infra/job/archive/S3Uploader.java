package org.apache.ambari.infra.job.archive;

import static org.apache.commons.lang.StringUtils.isNotBlank;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Optional;

import org.apache.ambari.infra.conf.security.CompositePasswordStore;
import org.apache.ambari.infra.conf.security.PasswordStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xmlpull.v1.XmlPullParserException;

import io.minio.MinioClient;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidArgumentException;
import io.minio.errors.InvalidBucketNameException;
import io.minio.errors.InvalidEndpointException;
import io.minio.errors.InvalidPortException;
import io.minio.errors.NoResponseException;
import io.minio.errors.RegionConflictException;

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
public class S3Uploader extends AbstractFileAction {

  private static final Logger LOG = LoggerFactory.getLogger(S3Uploader.class);

//  private final AmazonS3Client client;
  private final MinioClient client;
  private final String keyPrefix;
  private final String bucketName;

  public S3Uploader(S3Properties s3Properties, PasswordStore passwordStore) {
    LOG.info("Initializing S3 client with " + s3Properties);

    this.keyPrefix = s3Properties.getS3KeyPrefix();
    this.bucketName = s3Properties.getS3BucketName();

    PasswordStore compositePasswordStore = passwordStore;
    if (isNotBlank((s3Properties.getS3AccessFile())))
      compositePasswordStore = new CompositePasswordStore(passwordStore, S3AccessCsv.file(s3Properties.getS3AccessFile()));

    try {
      client = new MinioClient(s3Properties.getS3EndPoint(), compositePasswordStore.getPassword(S3AccessKeyNames.AccessKeyId.getEnvVariableName())
              .orElseThrow(() -> new IllegalArgumentException("Access key Id is not present!")),
              compositePasswordStore.getPassword(S3AccessKeyNames.SecretAccessKey.getEnvVariableName())
                      .orElseThrow(() -> new IllegalArgumentException("Secret Access Key is not present!")));

      if (!client.bucketExists(bucketName))
        client.makeBucket(bucketName);

    } catch (RegionConflictException | XmlPullParserException | InvalidBucketNameException | NoSuchAlgorithmException | InsufficientDataException | ErrorResponseException | InvalidKeyException | NoResponseException | InvalidPortException | InvalidEndpointException | InternalException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

//    BasicAWSCredentials credentials = new BasicAWSCredentials(
//            compositePasswordStore.getPassword(S3AccessKeyNames.AccessKeyId.getEnvVariableName())
//                    .orElseThrow(() -> new IllegalArgumentException("Access key Id is not present!")),
//            compositePasswordStore.getPassword(S3AccessKeyNames.SecretAccessKey.getEnvVariableName())
//                    .orElseThrow(() -> new IllegalArgumentException("Secret Access Key is not present!")));
//    client = new AmazonS3Client(credentials);
//    if (!isBlank(s3Properties.getS3EndPoint()))
//      client.setEndpoint(s3Properties.getS3EndPoint());
//     Note: without pathStyleAccess=true endpoint going to be <bucketName>.<host>:<port>
//    client.setS3ClientOptions(S3ClientOptions.builder().setPathStyleAccess(true).build());
  }

  @Override
  public File onPerform(File inputFile) {
    String key = keyPrefix + inputFile.getName();

    try {
      if (client.listObjects(bucketName, key).iterator().hasNext()) {
        throw new UnsupportedOperationException(String.format("Object '%s' already exists in bucket '%s'", key, bucketName));
      }

      try (FileInputStream fileInputStream = new FileInputStream(inputFile)) {
        client.putObject(bucketName, key, fileInputStream, inputFile.length(), "application/json");
        return inputFile;
      }
    } catch (InvalidKeyException | NoSuchAlgorithmException | NoResponseException | XmlPullParserException | InvalidArgumentException | InvalidBucketNameException | ErrorResponseException | InternalException | InsufficientDataException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static void main(String[] args) {
    S3Properties s3Properties = new S3Properties(null, "my_", "testbucket", "http://localhost:4569");
    S3Uploader uploader = new S3Uploader(s3Properties, propertyName -> Optional.of("test"));
    uploader.onPerform(new File("/Users/kkasa/project/ambari-infra/ambari-infra-manager/docker/test.txt"));
  }
}
