package com.parquet.engine.query.service;

import java.util.List;
import java.util.Map;
import com.parquet.engine.query.dto.request.IngestRequest;
import com.parquet.engine.query.dto.request.ServiceRequest;
import com.parquet.engine.query.dto.response.ServiceResponse;

public interface ParquetService {

  ServiceResponse<List<Map<String, Object>>> queryParquetFileByCountry(String fileName,
      String country);


  ServiceResponse<Void> ingestIntoDuckDbTable(ServiceRequest<IngestRequest> serviceRequest);

  ServiceResponse<List<Map<String, Object>>> queryDuckDbTable(String firstName, String country);
  
  ServiceResponse<Void> ingestAllFiles();

}
