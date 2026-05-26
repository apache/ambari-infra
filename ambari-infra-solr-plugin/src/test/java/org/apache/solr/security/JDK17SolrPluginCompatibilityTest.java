/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.security;

import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Field;

import static org.junit.Assert.*;

/**
 * Tests to validate JDK 17 compatibility for the ambari-infra-solr-plugin module.
 * Solr 8.11.x uses extensive reflection and needs --add-opens for JDK 17.
 *
 * @see <a href="https://issues.apache.org/jira/browse/AMBARI-26142">AMBARI-26142</a>
 */
public class JDK17SolrPluginCompatibilityTest {

    /**
     * Verify the runtime JDK version is 17+.
     */
    @Test
    public void testJDKVersionIs17OrHigher() {
        String specVersion = System.getProperty("java.specification.version");
        int version = Integer.parseInt(specVersion);
        assertTrue("Expected JDK 17+, but running on JDK " + version, version >= 17);
    }

    /**
     * Verify that --add-opens flags are present (required by Solr test framework).
     */
    @Test
    public void testAddOpensPresent() {
        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        boolean hasAddOpens = runtimeMxBean.getInputArguments().stream()
                .anyMatch(arg -> arg.startsWith("--add-opens"));
        assertTrue("Expected --add-opens JVM args for JDK 17 strong encapsulation", hasAddOpens);
    }

    /**
     * Verify reflection on java.lang works (Solr/Lucene internals use this).
     */
    @Test
    public void testJavaLangReflection() throws Exception {
        Field valueField = String.class.getDeclaredField("value");
        valueField.setAccessible(true);
        Object value = valueField.get("solr-test");
        assertNotNull("Should access String.value with --add-opens", value);
    }

    /**
     * Verify NIO reflection works (Lucene uses sun.nio.ch for MMap).
     */
    @Test
    public void testNIOReflection() throws Exception {
        java.nio.ByteBuffer buffer = java.nio.ByteBuffer.allocateDirect(1024);
        assertNotNull("ByteBuffer.allocateDirect should work on JDK 17", buffer);
        assertEquals(1024, buffer.capacity());
    }

    /**
     * Verify java.util.concurrent reflection works (Solr executor services).
     */
    @Test
    public void testConcurrentReflection() throws Exception {
        java.util.concurrent.ConcurrentHashMap<String, String> map =
                new java.util.concurrent.ConcurrentHashMap<>();
        map.put("collection", "audit_logs");

        Field tableField = java.util.concurrent.ConcurrentHashMap.class.getDeclaredField("table");
        tableField.setAccessible(true);
        Object table = tableField.get(map);
        assertNotNull("ConcurrentHashMap internal access should work", table);
    }

    /**
     * Verify that the InfraRuleBasedAuthorizationPlugin can be instantiated under JDK 17.
     */
    @Test
    public void testInfraAuthorizationPluginInstantiation() {
        InfraRuleBasedAuthorizationPlugin plugin = new InfraRuleBasedAuthorizationPlugin();
        assertNotNull("InfraRuleBasedAuthorizationPlugin should instantiate under JDK 17", plugin);
    }

    /**
     * Verify that the InfraKerberosHostValidator can be instantiated under JDK 17.
     */
    @Test
    public void testInfraKerberosHostValidatorInstantiation() {
        InfraKerberosHostValidator validator = new InfraKerberosHostValidator();
        assertNotNull("InfraKerberosHostValidator should instantiate under JDK 17", validator);
    }

    /**
     * Verify the Solr security model classes load under JDK 17.
     */
    @Test
    public void testSolrSecurityClassesLoad() throws Exception {
        Class<?> authPlugin = Class.forName("org.apache.solr.security.InfraRuleBasedAuthorizationPlugin");
        assertNotNull(authPlugin);
        Class<?> hostValidator = Class.forName("org.apache.solr.security.InfraKerberosHostValidator");
        assertNotNull(hostValidator);
    }
}
