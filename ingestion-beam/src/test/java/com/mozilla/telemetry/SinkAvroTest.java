/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry;

import static com.mozilla.telemetry.matchers.Lines.matchesInAnyOrder;
import static org.junit.Assert.assertThat;

import com.google.common.io.Resources;
import com.mozilla.telemetry.matchers.Lines;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

/** 
   The Avro tests are more involved than the file-based tests in SinkMainTest because there are more preconditions 
   necessary for testing. Each of the documents require the following metadata to be attached to the payload:

   - document_namespace
   - document_type
   - document_version

   The documents will need to have a corresponding store. Here, we will be testing a simple document containing
   integers.
 */
public class SinkAvroTest {

  @Rule
  public TemporaryFolder outputFolder = new TemporaryFolder();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private String outputPath;

  @Before
  public void initialize() {
    outputPath = outputFolder.getRoot().getAbsolutePath();
  }

  @Test
  public void testJsonToAvro() throws IOException {
    String input = Resources.getResource("testdata/avro-message-single-doctype.ndjson").getPath();
    String schemas = Resources.getResource("testdata/avro-schema-test.tar.gz").getPath();
    String output = outputPath + "/out";
    String errorOutput = outputPath + "/err";

    Sink.main(new String[] { "--inputFileFormat=json", "--inputType=file", "--input=" + input,
        "--outputType=avro", "--output=" + output, "--outputFileCompression=UNCOMPRESSED",
        "--schemaLocation=" + schemas, "--errorOutputFileCompression=UNCOMPRESSED",
        "--errorOutputType=file", "--errorOutput=" + errorOutput, });

    Path path = Paths.get(outputPath);
    Files.walk(path).filter(Files::isRegularFile).forEach(System.out::println);
  }
}