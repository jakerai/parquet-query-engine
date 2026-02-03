package com.parquet.engine.query.aws;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import com.parquet.engine.query.config.ParquetProperties;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

@Configuration
public class AwsConfig {

  private final ParquetProperties parquetProps;

  public AwsConfig(ParquetProperties parquetProps) {
    this.parquetProps = parquetProps;
  }

  /**
   * AWS SDK DefaultCredentialsProvider automatically picks up credentials from: Environment
   * variables: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY or ~/.aws/credentials file or IAM roles
   * 
   */
  private DefaultCredentialsProvider credentialsProvider() {
    return DefaultCredentialsProvider.builder().build();
  }

  @Bean
  public S3Client s3Client() {
    return S3Client.builder().credentialsProvider(credentialsProvider())
        .region(Region.of(parquetProps.getAws().getRegion())).build();
  }

}
