package com.parquet.engine.query.config;

import javax.sql.DataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import com.parquet.engine.query.source.ParquetSource;
import com.parquet.engine.query.source.factory.ParquetSourceFactory;

@Configuration
public class DuckDBConfig {

  @Bean
  public JdbcTemplate jdbcTemplate(DataSource duckDbDataSource) {
    return new JdbcTemplate(duckDbDataSource);
  }

  @Bean
  public ParquetSource parquetSource(ParquetSourceFactory factory) {
    return factory.getSource();
  }

}
