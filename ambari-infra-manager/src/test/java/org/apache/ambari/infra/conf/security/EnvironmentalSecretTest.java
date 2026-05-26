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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Optional;

import org.junit.Test;

public class EnvironmentalSecretTest {

  @Test
  public void testGetReturnsEmptyWhenEnvVarNotSet() {
    EnvironmentalSecret secret = new EnvironmentalSecret("AMBARI_INFRA_NONEXISTENT_VAR_12345");
    Optional<String> result = secret.get();
    assertThat(result.isPresent(), is(false));
  }

  @Test
  public void testGetReturnsValueWhenEnvVarExists() {
    // PATH is always set on all systems
    EnvironmentalSecret secret = new EnvironmentalSecret("PATH");
    Optional<String> result = secret.get();
    assertThat(result.isPresent(), is(true));
    assertTrue(result.get().length() > 0);
  }

  @Test
  public void testGetReturnsValueForHomeEnvVar() {
    // HOME is always set on Unix systems
    EnvironmentalSecret secret = new EnvironmentalSecret("HOME");
    Optional<String> result = secret.get();
    assertThat(result.isPresent(), is(true));
    assertTrue(result.get().length() > 0);
  }

  @Test
  public void testGetReturnsEmptyForEmptyVarName() {
    EnvironmentalSecret secret = new EnvironmentalSecret("");
    Optional<String> result = secret.get();
    assertThat(result.isPresent(), is(false));
  }
}
