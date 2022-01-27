/*
 * Copyright (C) 2022 Thomas Akehurst
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.tomakehurst.wiremock.common.filesource.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.github.tomakehurst.wiremock.common.BinaryFile;
import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.common.TextFile;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3FileSource implements FileSource {
  private static final Logger log = LoggerFactory.getLogger(S3FileSource.class);

  private final AmazonS3 s3;
  private final URI root;

  public S3FileSource(URI root) {
    s3 = AmazonS3ClientBuilder.standard().build();
    this.root = root;
  }

  public S3FileSource(AmazonS3 s3, URI root) {
    this.s3 = s3;
    this.root = root;
  }

  public S3FileSource(AmazonS3 s3, URI root, String subDir) {
    this.s3 = s3;
    this.root = build(root, subDir);
  }

  private static URI build(URI root, String key) {
    key = StringUtils.removeStart(key, "/");
    key = StringUtils.removeEnd(key, "/");
    return URI.create(root.toString() + "/" + key);
  }

  @Override
  public BinaryFile getBinaryFileNamed(String name) {
    return new S3BinaryFile(s3, build(root, name));
  }

  @Override
  public TextFile getTextFileNamed(String name) {
    return new S3TextFile(s3, build(root, name));
  }

  @Override
  public void createIfNecessary() {
    // noop, S3 will create files as needed
  }

  @Override
  public FileSource child(String subDirectoryName) {
    return new S3FileSource(s3, root, subDirectoryName);
  }

  @Override
  public String getPath() {
    return getUri().getPath();
  }

  @Override
  public URI getUri() {
    return root;
  }

  @Override
  public List<TextFile> listFilesRecursively() {
    List<TextFile> list = new ArrayList<>();
    URI uri = getUri();
    ListObjectsV2Request req = new ListObjectsV2Request();
    req.setBucketName(uri.getHost());
    req.setPrefix(uri.getPath());
    req.setMaxKeys(1000);
    // prevent endless loop my restricting to 100 pages (=100000 files)
    for (int i = 0; i < 100; i++) {
      log.info("Listing files (page {}) in {}", i, uri);
      ListObjectsV2Result result = s3.listObjectsV2(req);
      for (S3ObjectSummary os : result.getObjectSummaries()) {
        URI objUri = uriFromBucketKey(os.getBucketName(), os.getKey());
        list.add(new S3TextFile(s3, objUri));
      }
      if (result.getNextContinuationToken() == null) {
        break;
      }
      req.setContinuationToken(result.getNextContinuationToken());
    }

    return list;
  }

  private static URI uriFromBucketKey(String bucketName, String key) {
    key = StringUtils.removeStart(key, "/");
    key = StringUtils.removeEnd(key, "/");
    return URI.create("s3://" + bucketName + "/" + key);
  }

  @Override
  public void writeTextFile(String name, String contents) {
    URI uri = build(root, name);
    log.info("Writing text file {}", uri);
    s3.putObject(uri.getHost(), uri.getPath(), contents);
  }

  @Override
  public void writeBinaryFile(String name, byte[] contents) {
    URI uri = build(root, name);
    log.info("Writing binary file {}", uri);
    s3.putObject(uri.getHost(), uri.getPath(), new ByteArrayInputStream(contents), null);
  }

  @Override
  public boolean exists() {
    URI uri = getUri();
    log.info("Checking if {} exists", uri);
    return s3.doesObjectExist(uri.getHost(), uri.getPath());
  }

  @Override
  public void deleteFile(String name) {
    URI uri = getUri();
    log.info("Deleting {}", uri);
    s3.deleteObject(uri.getHost(), uri.getPath());
  }
}
