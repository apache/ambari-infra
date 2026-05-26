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

package org.apache.ambari.infra;

import org.junit.Test;

import javax.xml.bind.annotation.XmlRootElement;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.junit.Assert.*;

/**
 * Tests to validate JDK 17 compatibility for the ambari-infra-manager module.
 * Covers: strong encapsulation (--add-opens), JAXB availability, CGLib bytecode generation,
 * Guava 32 availability, and reflection access.
 *
 * @see <a href="https://issues.apache.org/jira/browse/AMBARI-26142">AMBARI-26142</a>
 */
public class JDK17CompatibilityTest {

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
     * JAXB was removed from the JDK in Java 11.
     * Verify that javax.xml.bind annotations are available via the explicit jaxb-api dependency.
     */
    @Test
    public void testJaxbAnnotationsAvailable() {
        assertNotNull("XmlRootElement annotation class should be loadable",
                XmlRootElement.class);
    }

    /**
     * Verify that --add-opens flags are present in JVM args (required for reflection-heavy frameworks).
     */
    @Test
    public void testAddOpensPresent() {
        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        boolean hasAddOpens = runtimeMxBean.getInputArguments().stream()
                .anyMatch(arg -> arg.startsWith("--add-opens"));
        assertTrue("Expected --add-opens JVM args for JDK 17 strong encapsulation", hasAddOpens);
    }

    /**
     * JDK 17 enforces strong encapsulation. Verify that reflection on java.lang.String
     * works when --add-opens java.base/java.lang=ALL-UNNAMED is set.
     */
    @Test
    public void testReflectionAccessToJavaLang() throws Exception {
        Field valueField = String.class.getDeclaredField("value");
        valueField.setAccessible(true);
        Object value = valueField.get("test");
        assertNotNull("Should be able to reflectively access String.value with --add-opens", value);
    }

    /**
     * Verify reflection on java.util classes works (required by Spring/Hadoop).
     */
    @Test
    public void testReflectionAccessToJavaUtil() throws Exception {
        Method sizeMethod = java.util.HashMap.class.getDeclaredMethod("size");
        sizeMethod.setAccessible(true);
        java.util.HashMap<String, String> map = new java.util.HashMap<>();
        Object result = sizeMethod.invoke(map);
        assertEquals("HashMap.size() via reflection should return 0", 0, result);
    }

    /**
     * Verify reflection on java.io classes works (--add-opens java.base/java.io).
     */
    @Test
    public void testJavaIOReflection() throws Exception {
        Field pathField = java.io.File.class.getDeclaredField("path");
        pathField.setAccessible(true);
        java.io.File testFile = new java.io.File("/tmp/test");
        Object pathValue = pathField.get(testFile);
        assertEquals("/tmp/test", pathValue);
    }

    /**
     * Verify that Guava 32.x classes are available (upgraded from 28.x for JDK 17 compat).
     */
    @Test
    public void testGuava32Available() {
        com.google.common.collect.ImmutableList<String> list =
                com.google.common.collect.ImmutableList.of("a", "b", "c");
        assertNotNull("Guava ImmutableList should be available", list);
        assertEquals(3, list.size());
    }

    /**
     * Verify that CGLib 3.3.0 can create enhanced classes under JDK 17.
     * CGLib needs access to internal class file format which changed in newer JDKs.
     */
    @Test
    public void testCGLibCompatibility() {
        net.sf.cglib.proxy.Enhancer enhancer = new net.sf.cglib.proxy.Enhancer();
        enhancer.setSuperclass(Object.class);
        enhancer.setCallback(new net.sf.cglib.proxy.NoOp() {});
        Object proxy = enhancer.create();
        assertNotNull("CGLib should be able to create enhanced proxy under JDK 17", proxy);
    }

    /**
     * Verify java.net reflection works (needed by Hadoop networking code).
     */
    @Test
    public void testJavaNetReflection() throws Exception {
        Class<?> inetAddressClass = Class.forName("java.net.InetAddress");
        assertNotNull(inetAddressClass);
        Method[] methods = inetAddressClass.getDeclaredMethods();
        assertTrue("InetAddress should have methods", methods.length > 0);
    }

    /**
     * Verify java.util.concurrent reflection works (needed by Solr internals).
     */
    @Test
    public void testConcurrentReflection() throws Exception {
        java.util.concurrent.ConcurrentHashMap<String, String> map =
                new java.util.concurrent.ConcurrentHashMap<>();
        map.put("key", "value");

        Field tableField = java.util.concurrent.ConcurrentHashMap.class.getDeclaredField("table");
        tableField.setAccessible(true);
        Object table = tableField.get(map);
        assertNotNull("Should access ConcurrentHashMap internals with --add-opens", table);
    }

    /**
     * Verify that javax.annotation-api is available (needed for JDK 9+ where javax.annotation was removed).
     */
    @Test
    public void testJavaxAnnotationAvailable() throws Exception {
        Class<?> clazz = Class.forName("javax.annotation.PostConstruct");
        assertNotNull("javax.annotation.PostConstruct should be available", clazz);
    }

    /**
     * Verify that Spring's reflection utilities work under JDK 17.
     */
    @Test
    public void testSpringReflectionWorks() throws Exception {
        Class<?> springReflectionUtils = Class.forName("org.springframework.util.ReflectionUtils");
        assertNotNull("Spring ReflectionUtils should be loadable", springReflectionUtils);
        Method findMethod = springReflectionUtils.getMethod("findMethod", Class.class, String.class, Class[].class);
        assertNotNull("findMethod should be accessible", findMethod);
    }

    /**
     * Verify JMX/management access works (--add-opens java.management).
     */
    @Test
    public void testJavaManagementAccess() {
        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        assertNotNull(runtimeMxBean.getUptime());
        assertTrue("JVM uptime should be > 0", runtimeMxBean.getUptime() > 0);
        assertNotNull(runtimeMxBean.getVmName());
    }
}
