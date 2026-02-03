package com.parquet.engine.query.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import lombok.Getter;
import lombok.Setter;

/**
 * @author Vishal Rai
 * 
 */

@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "parquet")
public class ParquetProperties {

  /**
   * source = local or s3
   */
  private String source;

  private Local local;
  private Aws aws;

  @Getter
  @Setter
  public static class Local {
    private String inputDir;
    private String processedDir;
    private String failedDir;
  }

  @Getter
  @Setter
  public static class Aws {
    private String region;
    private String bucket;
    private String inputDir;
    private String processedDir;
    private String failedDir;
  }

}
