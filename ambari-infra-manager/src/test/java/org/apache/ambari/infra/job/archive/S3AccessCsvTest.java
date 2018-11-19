package org.apache.ambari.infra.job.archive;

import static org.apache.ambari.infra.job.archive.S3AccessCsv.ACCESS_KEY_ID;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.io.StringReader;

import org.junit.Test;

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
public class S3AccessCsvTest {

  private static final String VALID_ACCESS_FILE = "Access key ID,Secret access key\n" +
          "someKey,someSecret\n";

  private static final String ANY_CSV_FILE = "Column1,Column2\n" +
          "Foo,Bar\n";

  @Test(expected = S3AccessCsvFormatException.class)
  public void testGetPasswordReturnsNullIfInputIsEmpty() {
    S3AccessCsv accessCsv = new S3AccessCsv(new StringReader(""), ACCESS_KEY_ID);
    assertThat(accessCsv.get().isPresent(), is(false));
  }

  @Test
  public void testGetPasswordReturnsAccessAndSecretKeyIfInputIsAValidS3AccessFile() {
    S3AccessCsv accessCsv = new S3AccessCsv(new StringReader(VALID_ACCESS_FILE), ACCESS_KEY_ID);
    assertThat(accessCsv.get().get(), is("someKey"));
  }

  @Test(expected = S3AccessCsvFormatException.class)
  public void testGetPasswordThrowsExceptionIfNotAValidS3AccessFileProvided() {
    S3AccessCsv accessCsv = new S3AccessCsv(new StringReader(ANY_CSV_FILE), ACCESS_KEY_ID);
    assertThat(accessCsv.get().isPresent(), is(false));
  }

  @Test(expected = S3AccessCsvFormatException.class)
  public void testGetPasswordThrowsExceptionIfAHeaderOnlyS3AccessFileProvided() {
    S3AccessCsv accessCsv = new S3AccessCsv(new StringReader("Access key ID,Secret access key\n"), ACCESS_KEY_ID);
    assertThat(accessCsv.get().isPresent(), is(false));
  }

  @Test(expected = S3AccessCsvFormatException.class)
  public void testGetPasswordThrowsExceptionIfOnlyOneValidColumnProvided() {
    S3AccessCsv accessCsv = new S3AccessCsv(new StringReader("Access key ID,Column\n"), ACCESS_KEY_ID);
    assertThat(accessCsv.get().isPresent(), is(false));
  }
}