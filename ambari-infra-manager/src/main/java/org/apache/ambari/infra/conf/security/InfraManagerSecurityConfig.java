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
package org.apache.ambari.infra.conf.security;

import static org.apache.ambari.infra.conf.security.HadoopCredentialStore.CREDENTIAL_STORE_PROVIDER_PATH_PROPERTY;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class InfraManagerSecurityConfig {

  @Value("${"+ CREDENTIAL_STORE_PROVIDER_PATH_PROPERTY + ":}")
  private String credentialStoreProviderPath;

  @Bean
  public HadoopCredentialStore hadoopCredentialStore() {
    return new HadoopCredentialStore(credentialStoreProviderPath);
  }

  @Bean
  public S3Secrets s3SecretStore(HadoopCredentialStore hadoopCredentialStore) {
    return new S3Secrets(s3AccessKeyId(hadoopCredentialStore), s3SecretKeyId(hadoopCredentialStore));
  }

  private Secret s3AccessKeyId(HadoopCredentialStore hadoopCredentialStore) {
    return new CompositeSecret(
            hadoopCredentialStore.getSecret( "AWS_ACCESS_KEY_ID"),
            new EnvironmentalSecret("AWS_ACCESS_KEY_ID"));
  }

  private Secret s3SecretKeyId(HadoopCredentialStore hadoopCredentialStore) {
    return new CompositeSecret(
            hadoopCredentialStore.getSecret( "AWS_SECRET_ACCESS_KEY"),
            new EnvironmentalSecret("AWS_SECRET_ACCESS_KEY"));
  }

  @Bean
  public SslSecrets sslSecrets(HadoopCredentialStore hadoopCredentialStore) {
    return new SslSecrets(
            hadoopCredentialStore.getSecret("infra_manager_keystore_password"),
            hadoopCredentialStore.getSecret("infra_manager_truststore_password"));
  }
}
