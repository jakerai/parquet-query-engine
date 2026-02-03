package com.parquet.engine.query.source;

import java.nio.file.Path;
import java.util.List;

public interface ParquetSource {

  Path getParquetPath(String fileName) throws Exception;

  List<String> listFileNames();

  boolean moveFileToProcessedDir(String fileName);
  
  boolean moveFileToFailedDir(String fileName);
}
