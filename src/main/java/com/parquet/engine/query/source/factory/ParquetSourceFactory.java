package com.parquet.engine.query.source.factory;

import org.springframework.stereotype.Component;
import com.parquet.engine.query.config.ParquetProperties;
import com.parquet.engine.query.source.ParquetSource;
import com.parquet.engine.query.source.impl.AwsS3ParquetSource;
import com.parquet.engine.query.source.impl.LocalParquetSource;
import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class ParquetSourceFactory {

  private final ParquetProperties props;

  private final LocalParquetSource localParquetLoader;
  private final AwsS3ParquetSource awsS3ParquetSource;

  public ParquetSource getSource() {
    return switch (props.getSource().toLowerCase()) {
      case "local" -> localParquetLoader;
      case "aws" -> awsS3ParquetSource;
      default -> throw new IllegalStateException("Unsupported data source: " + props.getSource());
    };
  }
}
