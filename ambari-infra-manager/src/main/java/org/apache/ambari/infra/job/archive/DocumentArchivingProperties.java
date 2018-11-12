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

import static org.apache.ambari.infra.json.StringToDurationConverter.toDuration;
import static org.apache.ambari.infra.json.StringToFsPermissionConverter.toFsPermission;
import static org.apache.commons.lang.StringUtils.isBlank;

import java.time.Duration;

import org.apache.ambari.infra.job.JobProperties;
import org.apache.ambari.infra.json.DurationToStringConverter;
import org.apache.ambari.infra.json.FsPermissionToStringConverter;
import org.apache.hadoop.fs.permission.FsPermission;
import org.springframework.batch.core.JobParameters;

public class DocumentArchivingProperties extends JobProperties<ArchivingParameters> {
  private int readBlockSize;
  private int writeBlockSize;
  private ExportDestination destination;
  private String localDestinationDirectory;
  private String fileNameSuffixColumn;
  private String fileNameSuffixDateFormat;
  private Duration ttl;
  private SolrProperties solr;

  private String s3AccessFile;
  private String s3KeyPrefix;
  private String s3BucketName;
  private String s3Endpoint;

  private String hdfsEndpoint;
  private String hdfsDestinationDirectory;
  private FsPermission hdfsFilePermission;
  private String hdfsKerberosPrincipal;
  private String hdfsKerberosKeytabPath;

  public int getReadBlockSize() {
    return readBlockSize;
  }

  public void setReadBlockSize(int readBlockSize) {
    this.readBlockSize = readBlockSize;
  }

  public int getWriteBlockSize() {
    return writeBlockSize;
  }

  public void setWriteBlockSize(int writeBlockSize) {
    this.writeBlockSize = writeBlockSize;
  }

  public ExportDestination getDestination() {
    return destination;
  }

  public void setDestination(ExportDestination destination) {
    this.destination = destination;
  }

  public String getLocalDestinationDirectory() {
    return localDestinationDirectory;
  }

  public void setLocalDestinationDirectory(String localDestinationDirectory) {
    this.localDestinationDirectory = localDestinationDirectory;
  }

  public String getFileNameSuffixColumn() {
    return fileNameSuffixColumn;
  }

  public void setFileNameSuffixColumn(String fileNameSuffixColumn) {
    this.fileNameSuffixColumn = fileNameSuffixColumn;
  }

  public String getFileNameSuffixDateFormat() {
    return fileNameSuffixDateFormat;
  }

  public void setFileNameSuffixDateFormat(String fileNameSuffixDateFormat) {
    this.fileNameSuffixDateFormat = fileNameSuffixDateFormat;
  }

  public Duration getTtl() {
    return ttl;
  }

  public void setTtl(Duration ttl) {
    this.ttl = ttl;
  }

  public SolrProperties getSolr() {
    return solr;
  }

  public void setSolr(SolrProperties query) {
    this.solr = query;
  }

  public String getS3AccessFile() {
    return s3AccessFile;
  }

  public void setS3AccessFile(String s3AccessFile) {
    this.s3AccessFile = s3AccessFile;
  }

  public String getS3KeyPrefix() {
    return s3KeyPrefix;
  }

  public void setS3KeyPrefix(String s3KeyPrefix) {
    this.s3KeyPrefix = s3KeyPrefix;
  }

  public String getS3BucketName() {
    return s3BucketName;
  }

  public void setS3BucketName(String s3BucketName) {
    this.s3BucketName = s3BucketName;
  }

  public String getS3Endpoint() {
    return s3Endpoint;
  }

  public void setS3Endpoint(String s3Endpoint) {
    this.s3Endpoint = s3Endpoint;
  }

  public String getHdfsEndpoint() {
    return hdfsEndpoint;
  }

  public void setHdfsEndpoint(String hdfsEndpoint) {
    this.hdfsEndpoint = hdfsEndpoint;
  }

  public String getHdfsDestinationDirectory() {
    return hdfsDestinationDirectory;
  }

  public FsPermission getHdfsFilePermission() {
    return hdfsFilePermission;
  }

  public void setHdfsFilePermission(FsPermission hdfsFilePermission) {
    this.hdfsFilePermission = hdfsFilePermission;
  }

  public void setHdfsDestinationDirectory(String hdfsDestinationDirectory) {
    this.hdfsDestinationDirectory = hdfsDestinationDirectory;
  }

  public String getHdfsKerberosPrincipal() {
    return hdfsKerberosPrincipal;
  }

  public void setHdfsKerberosPrincipal(String hdfsKerberosPrincipal) {
    this.hdfsKerberosPrincipal = hdfsKerberosPrincipal;
  }

  public String getHdfsKerberosKeytabPath() {
    return hdfsKerberosKeytabPath;
  }

  public void setHdfsKerberosKeytabPath(String hdfsKerberosKeytabPath) {
    this.hdfsKerberosKeytabPath = hdfsKerberosKeytabPath;
  }

  private int getIntJobParameter(JobParameters jobParameters, String parameterName, int defaultValue) {
    String valueText = jobParameters.getString(parameterName);
    if (isBlank(valueText))
      return defaultValue;
    return Integer.parseInt(valueText);
  }

  @Override
  public ArchivingParameters merge(JobParameters jobParameters) {
    ArchivingParameters archivingParameters = new ArchivingParameters();
    archivingParameters.setReadBlockSize(getIntJobParameter(jobParameters, "readBlockSize", readBlockSize));
    archivingParameters.setWriteBlockSize(getIntJobParameter(jobParameters, "writeBlockSize", writeBlockSize));
    archivingParameters.setDestination(ExportDestination.valueOf(jobParameters.getString("destination", destination.name())));
    archivingParameters.setLocalDestinationDirectory(jobParameters.getString("localDestinationDirectory", localDestinationDirectory));
    archivingParameters.setFileNameSuffixColumn(jobParameters.getString("fileNameSuffixColumn", fileNameSuffixColumn));
    archivingParameters.setFileNameSuffixDateFormat(jobParameters.getString("fileNameSuffixDateFormat", fileNameSuffixDateFormat));
    archivingParameters.setS3AccessFile(jobParameters.getString("s3AccessFile", s3AccessFile));
    archivingParameters.setS3BucketName(jobParameters.getString("s3BucketName", s3BucketName));
    archivingParameters.setS3KeyPrefix(jobParameters.getString("s3KeyPrefix", s3KeyPrefix));
    archivingParameters.setS3Endpoint(jobParameters.getString("s3Endpoint", s3Endpoint));
    archivingParameters.setHdfsEndpoint(jobParameters.getString("hdfsEndpoint", hdfsEndpoint));
    archivingParameters.setHdfsDestinationDirectory(jobParameters.getString("hdfsDestinationDirectory", hdfsDestinationDirectory));
    archivingParameters.setHdfsFilePermission(toFsPermission(jobParameters.getString("hdfsFilePermission", FsPermissionToStringConverter.toString(hdfsFilePermission))));
    archivingParameters.setHdfsKerberosPrincipal(jobParameters.getString("hdfsKerberosPrincipal", hdfsKerberosPrincipal));
    archivingParameters.setHdfsKerberosKeytabPath(jobParameters.getString("hdfsKerberosKeytabPath", hdfsKerberosKeytabPath));
    archivingParameters.setSolr(solr.merge(jobParameters));
    archivingParameters.setStart(jobParameters.getString("start"));
    archivingParameters.setEnd(jobParameters.getString("end"));
    archivingParameters.setTtl(toDuration(jobParameters.getString("ttl", DurationToStringConverter.toString(ttl))));
    return archivingParameters;
  }
}
