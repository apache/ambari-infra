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

import static org.easymock.EasyMock.expect;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.io.File;

import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(EasyMockRunner.class)
public class CompositeFileActionTest extends EasyMockSupport {

  @Mock
  private FileAction action1;
  @Mock
  private FileAction action2;
  @Mock
  private FileAction action3;

  @After
  public void tearDown() {
    verifyAll();
  }

  @Test
  public void testPerformWithNoActions() {
    replayAll();
    CompositeFileAction compositeAction = new CompositeFileAction();
    File inputFile = new File("/tmp/input.json");
    File result = compositeAction.perform(inputFile);
    assertThat(result, is(inputFile));
  }

  @Test
  public void testPerformWithSingleAction() {
    File inputFile = new File("/tmp/input.json");
    File outputFile = new File("/tmp/output.json.gz");

    expect(action1.perform(inputFile)).andReturn(outputFile);
    replayAll();

    CompositeFileAction compositeAction = new CompositeFileAction(action1);
    File result = compositeAction.perform(inputFile);
    assertThat(result, is(outputFile));
  }

  @Test
  public void testPerformChainsMultipleActions() {
    File inputFile = new File("/tmp/input.json");
    File intermediateFile = new File("/tmp/input.json.gz");
    File finalFile = new File("/tmp/uploaded.json.gz");

    expect(action1.perform(inputFile)).andReturn(intermediateFile);
    expect(action2.perform(intermediateFile)).andReturn(finalFile);
    replayAll();

    CompositeFileAction compositeAction = new CompositeFileAction(action1, action2);
    File result = compositeAction.perform(inputFile);
    assertThat(result, is(finalFile));
  }

  @Test
  public void testAddActionAppendToChain() {
    File inputFile = new File("/tmp/input.json");
    File file2 = new File("/tmp/step2.json");
    File file3 = new File("/tmp/step3.json");

    expect(action1.perform(inputFile)).andReturn(file2);
    expect(action2.perform(file2)).andReturn(file3);
    replayAll();

    CompositeFileAction compositeAction = new CompositeFileAction(action1);
    compositeAction.add(action2);
    File result = compositeAction.perform(inputFile);
    assertThat(result, is(file3));
  }

  @Test
  public void testPerformThreeActionsInSequence() {
    File inputFile = new File("/tmp/raw.json");
    File compressed = new File("/tmp/raw.json.bz2");
    File uploaded = new File("/hdfs/raw.json.bz2");
    File cleaned = new File("/tmp/done");

    expect(action1.perform(inputFile)).andReturn(compressed);
    expect(action2.perform(compressed)).andReturn(uploaded);
    expect(action3.perform(uploaded)).andReturn(cleaned);
    replayAll();

    CompositeFileAction compositeAction = new CompositeFileAction(action1, action2, action3);
    File result = compositeAction.perform(inputFile);
    assertThat(result, is(cleaned));
  }
}
