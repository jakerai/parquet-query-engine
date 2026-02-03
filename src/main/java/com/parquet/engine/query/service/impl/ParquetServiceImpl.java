package com.parquet.engine.query.service.impl;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import com.parquet.engine.query.dto.request.IngestRequest;
import com.parquet.engine.query.dto.request.ServiceRequest;
import com.parquet.engine.query.dto.response.ServiceResponse;
import com.parquet.engine.query.exception.NotFound;
import com.parquet.engine.query.service.ParquetService;
import com.parquet.engine.query.source.ParquetSource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Vishal Rai
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ParquetServiceImpl implements ParquetService {

  private final ParquetSource parquetSource;
  private final JdbcTemplate jdbcTemplate;
  private final String tableName = "report";


  @Override
  public ServiceResponse<List<Map<String, Object>>> queryParquetFileByCountry(String fileName,
      String country) {
    try {

      Path parquetPath = parquetSource.getParquetPath(fileName);

      String sql = """
          SELECT *
          FROM read_parquet('%s')
          WHERE country = '%s'
          """.formatted(parquetPath.toAbsolutePath(), country);

      return ServiceResponse.success(jdbcTemplate.queryForList(sql), "Fetched successfully");

    } catch (Exception e) {
      log.error("Failed to query parquet by country", e);
      throw new RuntimeException("Failed to query parquet by id and country", e);
    }
  }

  @Override
  public ServiceResponse<Void> ingestIntoDuckDbTable(ServiceRequest<IngestRequest> serviceRequest) {

    IngestRequest ingestRequest = serviceRequest.getPayload();
    ingestIntoDuckDbTable(ingestRequest.getFileName());
    return ServiceResponse.success("Parquet files ingested successfully");
  }



  @Override
  public ServiceResponse<List<Map<String, Object>>> queryDuckDbTable(String firstName,
      String country) {
    try {

      StringBuilder sql = new StringBuilder("SELECT * FROM " + tableName + " WHERE 1=1");
      List<Object> params = new ArrayList<>();

      if (firstName != null && !firstName.isBlank()) {
        sql.append(" AND first_name = ?");
        params.add(firstName);
      }

      if (country != null && !country.isBlank()) {
        sql.append(" AND country = ?");
        params.add(country);
      }

      List<Map<String, Object>> result =
          jdbcTemplate.queryForList(sql.toString(), params.toArray());
      if (result.size() <= 0) {
        throw new NotFound("No data found");
      }
      return ServiceResponse.success(result, "Fetched data from DuckDB table successfully");

    } catch (Exception e) {
      log.error("Failed to query DuckDB table '{}' with filters firstName={}, country={}",
          tableName, firstName, country, e);
      throw new RuntimeException("Failed to query data from DuckDB table", e);
    }
  }

  @Override
  public ServiceResponse<Void> ingestAllFiles() {

    log.info("Starting parquet ingestion job");

    List<String> fileNames = parquetSource.listFileNames();

    if (fileNames.isEmpty()) {
      log.info("No parquet files found for ingestion");
      return ServiceResponse.success("No Parquet files for ingestion");
    }

    log.info("Found {} parquet files to ingest", fileNames.size());

    for (String fileName : fileNames) {
      log.info("Processing parquet file: {}", fileName);

      try {
        // Ingesting into DuckDB
        ingestIntoDuckDbTable(fileName);

        // Moving to processed directory
        boolean moved = parquetSource.moveFileToProcessedDir(fileName);
        if (!moved) {
          log.warn("File ingested but failed to move to processed dir: {}", fileName);
        } else {
          log.info("Successfully ingested and moved file to processed dir: {}", fileName);
        }
        ServiceResponse.success("Ingested successfully");
      } catch (Exception e) {
        log.error("Failed to ingest parquet file: {}", fileName, e);

        // Moving to failed directory
        boolean movedToFailed = parquetSource.moveFileToFailedDir(fileName);
        if (!movedToFailed) {
          log.error("Failed to move file to failed dir: {}", fileName);
        }
      }
    }

    log.info("Parquet ingestion job completed");
    return ServiceResponse.success("Parquet files ingested successfully");
  }

  private void ingestIntoDuckDbTable(String fileName) {
    try {
      // Resolving parquet file path
      Path parquetPath = parquetSource.getParquetPath(fileName);
      if (Files.notExists(parquetPath)) {
        throw new RuntimeException(
            "Parquet file does not exist at path: " + parquetPath.toAbsolutePath());
      }

      log.info("Ingesting file '{}' into table '{}'", fileName, tableName);

      // Creating table matching Parquet schema
      String createTableSql = """
              CREATE TABLE IF NOT EXISTS %s (
                  id BIGINT PRIMARY KEY,
                  first_name VARCHAR,
                  age INTEGER,
                  country VARCHAR
              )
          """.formatted(tableName);

      jdbcTemplate.execute(createTableSql);

      // Escaping backslashes for Windows paths
      String parquetAbsolutePath = parquetPath.toAbsolutePath().toString().replace("\\", "\\\\");

      // Upserting parquet data using ON CONFLICT
      // Only including columns that exist in table
      String insertSql = """
              INSERT INTO %s (id, first_name, age, country)
              SELECT id, first_name, age, country FROM read_parquet('%s')
              ON CONFLICT(id) DO UPDATE SET
                  first_name = excluded.first_name,
                  age = excluded.age,
                  country = excluded.country
          """.formatted(tableName, parquetAbsolutePath);

      jdbcTemplate.execute(insertSql);

      log.info("Parquet file '{}' ingested into table '{}' successfully", fileName, tableName);

    } catch (Exception e) {
      log.error("Failed to ingest parquet file '{}' into table '{}'", fileName, tableName, e);
      throw new RuntimeException("Failed to ingest parquet file into DuckDB", e);
    }
  }


}

