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

import static org.apache.commons.csv.CSVFormat.DEFAULT;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import org.apache.ambari.infra.conf.security.Secret;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class S3AccessCsv implements Secret {
  private static final Logger logger = LogManager.getLogger(S3AccessCsv.class);
  public static final String ACCESS_KEY_ID = "Access key ID";
  public static final String SECRET_ACCESS_KEY = "Secret access key";


  public static S3AccessCsv file(String path, String propertyName) {
    try {
      return new S3AccessCsv(new FileReader(path), propertyName);
    } catch (FileNotFoundException e) {
      throw new UncheckedIOException(e);
    }
  }

  private final Reader reader;
  private final String propertyName;

  S3AccessCsv(Reader reader, String propertyName) {
    this.reader = reader;
    this.propertyName = propertyName;
  }

  @Override
  public Optional<String> get() {
    try (CSVParser csvParser = CSVParser.parse(reader, DEFAULT.withHeader(
            S3AccessKeyNames.AccessKeyId.getCsvName(), S3AccessKeyNames.SecretAccessKey.getCsvName()))) {
      Iterator<CSVRecord> iterator = csvParser.iterator();
      if (!iterator.hasNext()) {
        throw new S3AccessCsvFormatException("Csv file is empty!");
      }

      CSVRecord record = iterator.next();
      if (record.size() < 2) {
        throw new S3AccessCsvFormatException("Csv file contains less than 2 columns!");
      }

      checkColumnExists(record, ACCESS_KEY_ID);
      checkColumnExists(record, SECRET_ACCESS_KEY);

      if (!iterator.hasNext()) {
        throw new S3AccessCsvFormatException("Csv file contains header only!");
      }

      record = iterator.next();

      Map<String, Integer> header = csvParser.getHeaderMap();
      return Optional.ofNullable(record.get(header.get(propertyName)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void checkColumnExists(CSVRecord record, String s3AccessKeyName) {
    if (!s3AccessKeyName.equals(record.get(s3AccessKeyName))) {
      throw new S3AccessCsvFormatException(String.format("Csv file does not contain the required column: '%s'", s3AccessKeyName));
    }
  }
}
