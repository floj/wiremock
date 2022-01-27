package com.github.tomakehurst.wiremock.common.filesource.s3;

import java.net.URI;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;

public class BucketAndKey {

  private final String bucket;
  private final String key;

  public static BucketAndKey from(URI uri) {
    if (!uri.getScheme().equalsIgnoreCase("s3")) {
      throw new IllegalArgumentException("URI scheme must be s3://");
    }
    String bucket = uri.getHost();
    String key = StringUtils.removeStart(uri.getPath(), "/");
    return new BucketAndKey(bucket, key);
  }

  private BucketAndKey(String bucket, String key) {
    this.bucket = bucket;
    this.key = key;
  }

  public String getBucket() {
    return bucket;
  }

  public String getKey() {
    return key;
  }

  @Override
  public String toString() {
    return String.format("[s3://%s/%s]", bucket, key);
  }

  @Override
  public int hashCode() {
    return Objects.hash(bucket, key);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    BucketAndKey other = (BucketAndKey) obj;
    return Objects.equals(bucket, other.bucket) && Objects.equals(key, other.key);
  }
}
