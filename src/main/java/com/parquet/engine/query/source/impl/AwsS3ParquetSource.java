package com.parquet.engine.query.source.impl;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.springframework.stereotype.Component;
import com.parquet.engine.query.config.ParquetProperties;
import com.parquet.engine.query.source.ParquetSource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

@Slf4j
@Component
@RequiredArgsConstructor
public class AwsS3ParquetSource implements ParquetSource {

  private final ParquetProperties props;

  private final S3Client s3Client;

  public Path getParquetPath(String fileName) throws Exception {
    Path localFile = Path.of(props.getLocal().getInputDir(), fileName);

    if (Files.notExists(localFile)) {
      Files.createDirectories(localFile.getParent());
      try (InputStream s3Stream = s3Client.getObject(
          GetObjectRequest.builder().bucket(props.getAws().getBucket()).key(fileName).build())) {
        Files.copy(s3Stream, localFile);
      }
    }
    return localFile;
  }

  @Override
  public List<String> listFileNames() {

    String prefix = normalizePrefix(props.getAws().getInputDir());
    List<String> parquetFiles = new ArrayList<>();

    /* In AWS S3 pagination, null is the starting state */
    String continuationToken = null;

    do {
      ListObjectsV2Request request =
          ListObjectsV2Request.builder().bucket(props.getAws().getBucket()).prefix(prefix)
              .continuationToken(continuationToken).build();

      ListObjectsV2Response response = s3Client.listObjectsV2(request);

      if (response.contents() != null) {
        response.contents().stream().map(S3Object::key)
            .filter(key -> key.toLowerCase().endsWith(".parquet"))/* Only parquet */
            .map(key -> key.substring(key.lastIndexOf('/') + 1)).forEach(parquetFiles::add);
      }

      continuationToken = response.nextContinuationToken();

    } while (continuationToken != null);

    return parquetFiles;
  }


  private String normalizePrefix(String directoryName) {
    if (directoryName == null || directoryName.isBlank()) {
      return "";
    }
    return directoryName.endsWith("/") ? directoryName : directoryName + "/";
  }

  @Override
  public boolean moveFileToProcessedDir(String fileName) {

    if (fileName == null || fileName.isBlank()) {
      return false;
    }

    String bucket = props.getAws().getBucket();
    String sourcePrefix = normalizePrefix(props.getAws().getInputDir());
    String processedPrefix = normalizePrefix(props.getAws().getProcessedDir());

    String sourceKey = sourcePrefix + fileName;
    String destinationKey = processedPrefix + fileName;

    try {
      // Copying object
      CopyObjectRequest copyRequest = CopyObjectRequest.builder().sourceBucket(bucket)
          .sourceKey(sourceKey).destinationBucket(bucket).destinationKey(destinationKey).build();

      s3Client.copyObject(copyRequest);

      // Deleting original object
      DeleteObjectRequest deleteRequest =
          DeleteObjectRequest.builder().bucket(bucket).key(sourceKey).build();

      s3Client.deleteObject(deleteRequest);

      return true;

    } catch (S3Exception e) {
      log.error("Failed to move file {} to processed dir", fileName, e);
      return false;
    }
  }


  @Override
  public boolean moveFileToFailedDir(String fileName) {

    if (fileName == null || fileName.isBlank()) {
      return false;
    }

    String bucket = props.getAws().getBucket();
    String sourcePrefix = normalizePrefix(props.getAws().getInputDir());
    String failedPrefix = normalizePrefix(props.getAws().getFailedDir());

    String sourceKey = sourcePrefix + fileName;
    String destinationKey = failedPrefix + fileName;

    try {
      // Coping object
      s3Client.copyObject(CopyObjectRequest.builder().sourceBucket(bucket).sourceKey(sourceKey)
          .destinationBucket(bucket).destinationKey(destinationKey).build());

      // Deleting original
      s3Client.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(sourceKey).build());

      return true;

    } catch (S3Exception e) {
      log.error("Failed to move file {} to failed dir (S3)", fileName, e);
      return false;
    }
  }


}
