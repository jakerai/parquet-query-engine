package com.parquet.engine.query.controller;

import java.util.List;
import java.util.Map;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import com.parquet.engine.query.dto.request.IngestRequest;
import com.parquet.engine.query.dto.request.ServiceRequest;
import com.parquet.engine.query.dto.response.ServiceResponse;
import com.parquet.engine.query.service.ParquetService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;

@RequestMapping(path = "/parquet")
@RestController
@RequiredArgsConstructor
public class ParquetController {

  private final ParquetService duckDBService;


  @GetMapping(path = "/{fileName}", produces = {MediaType.APPLICATION_JSON_VALUE})

  public ResponseEntity<ServiceResponse<List<Map<String, Object>>>> getByCountry(
      @PathVariable String fileName, @RequestParam String country) {
    return ResponseEntity.ok(duckDBService.queryParquetFileByCountry(fileName, country));
  }


  @PostMapping(path = "/ingest", consumes = {MediaType.APPLICATION_JSON_VALUE},
      produces = {MediaType.APPLICATION_JSON_VALUE})
  public ResponseEntity<ServiceResponse<Void>> ingestByFileName(
      @Valid @RequestBody ServiceRequest<IngestRequest> serviceRequest) {

    return ResponseEntity.ok(duckDBService.ingestIntoDuckDbTable(serviceRequest));
  }
  
  @PostMapping(path = "/ingest-files", consumes = {MediaType.APPLICATION_JSON_VALUE},
      produces = {MediaType.APPLICATION_JSON_VALUE})
  public ResponseEntity<ServiceResponse<Void>> ingestFiles() {

    return ResponseEntity.ok(duckDBService.ingestAllFiles());
  }


  @GetMapping(path = "/ingest", produces = {MediaType.APPLICATION_JSON_VALUE})

  public ResponseEntity<ServiceResponse<List<Map<String, Object>>>> readIngestedTable(
      @RequestParam(name = "firstName", required = false) String firstName,
      @RequestParam(name = "country", required = false) String country) {

    return ResponseEntity.ok(duckDBService.queryDuckDbTable(firstName, country));
  }

}
