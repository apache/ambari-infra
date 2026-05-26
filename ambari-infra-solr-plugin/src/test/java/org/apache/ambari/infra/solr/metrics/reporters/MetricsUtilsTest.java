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
package org.apache.ambari.infra.solr.metrics.reporters;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.TreeMap;

import org.apache.hadoop.metrics2.sink.timeline.TimelineMetric;
import org.junit.Test;

public class MetricsUtilsTest {

  @Test
  public void testGetHostNameReturnsNonNull() {
    String hostName = MetricsUtils.getHostName();
    assertNotNull("getHostName() should not return null", hostName);
    assertTrue("hostName should not be empty", hostName.length() > 0);
  }

  @Test
  public void testToTimelineMetricCreatesValidMetric() {
    long currentMillis = System.currentTimeMillis();
    TimelineMetric metric = MetricsUtils.toTimelineMetric("infra.solr.requests", 100.0, currentMillis);

    assertNotNull(metric);
    assertEquals("infra.solr.requests", metric.getMetricName());
    assertEquals("ambari-infra-solr", metric.getAppId());
    assertEquals(currentMillis, metric.getStartTime());
    assertEquals("Long", metric.getType());
  }

  @Test
  public void testToTimelineMetricHasCorrectValues() {
    long currentMillis = 1700000000000L;
    double value = 42.5;
    TimelineMetric metric = MetricsUtils.toTimelineMetric("infra.solr.docs", value, currentMillis);

    TreeMap<Long, Double> metricValues = metric.getMetricValues();
    assertNotNull(metricValues);
    assertEquals(1, metricValues.size());
    assertEquals(Double.valueOf(value), metricValues.get(currentMillis));
  }

  @Test
  public void testToTimelineMetricWithZeroValue() {
    long currentMillis = System.currentTimeMillis();
    TimelineMetric metric = MetricsUtils.toTimelineMetric("infra.solr.errors", 0.0, currentMillis);

    assertNotNull(metric);
    assertEquals("infra.solr.errors", metric.getMetricName());
    TreeMap<Long, Double> metricValues = metric.getMetricValues();
    assertEquals(Double.valueOf(0.0), metricValues.get(currentMillis));
  }

  @Test
  public void testToTimelineMetricSetsHostName() {
    long currentMillis = System.currentTimeMillis();
    TimelineMetric metric = MetricsUtils.toTimelineMetric("infra.solr.test", 1.0, currentMillis);

    assertNotNull(metric.getHostName());
    assertEquals(MetricsUtils.getHostName(), metric.getHostName());
  }

  @Test
  public void testNamePrefixConstant() {
    assertEquals("infra.", MetricsUtils.NAME_PREFIX);
  }
}
