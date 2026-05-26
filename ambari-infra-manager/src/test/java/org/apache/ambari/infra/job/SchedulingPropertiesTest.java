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
package org.apache.ambari.infra.job;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

import org.junit.Test;

public class SchedulingPropertiesTest {

  @Test
  public void testDefaultValues() {
    SchedulingProperties properties = new SchedulingProperties();
    assertThat(properties.isEnabled(), is(false));
    assertThat(properties.getCron(), is(nullValue()));
  }

  @Test
  public void testSetEnabled() {
    SchedulingProperties properties = new SchedulingProperties();
    properties.setEnabled(true);
    assertThat(properties.isEnabled(), is(true));
  }

  @Test
  public void testSetCron() {
    SchedulingProperties properties = new SchedulingProperties();
    properties.setCron("0 0 * * * ?");
    assertThat(properties.getCron(), is("0 0 * * * ?"));
  }

  @Test
  public void testSetAndGetAllProperties() {
    SchedulingProperties properties = new SchedulingProperties();
    properties.setEnabled(true);
    properties.setCron("*/5 * * * * ?");

    assertThat(properties.isEnabled(), is(true));
    assertThat(properties.getCron(), is("*/5 * * * * ?"));
  }

  @Test
  public void testDisableAfterEnable() {
    SchedulingProperties properties = new SchedulingProperties();
    properties.setEnabled(true);
    assertThat(properties.isEnabled(), is(true));

    properties.setEnabled(false);
    assertThat(properties.isEnabled(), is(false));
  }
}
