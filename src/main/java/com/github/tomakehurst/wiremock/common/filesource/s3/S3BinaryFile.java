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
import com.amazonaws.services.s3.model.S3Object;
import com.github.tomakehurst.wiremock.common.BinaryFile;
import java.io.InputStream;
import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3BinaryFile extends BinaryFile {

  private static final Logger log = LoggerFactory.getLogger(S3FileSource.class);

  private final AmazonS3 s3;

  public S3BinaryFile(AmazonS3 s3, URI uri) {
    super(uri);
    this.s3 = s3;
  }

  @SuppressWarnings("resource")
  @Override
  public InputStream getStream() {
    BucketAndKey bk = BucketAndKey.from(getUri());
    log.info("Opening file {}", bk);

    S3Object obj = s3.getObject(bk.getBucket(), bk.getKey());
    return obj.getObjectContent();
  }
}
