package com.parquet.engine.query.source.impl;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.stream.Stream;
import org.springframework.stereotype.Component;
import com.parquet.engine.query.config.ParquetProperties;
import com.parquet.engine.query.exception.NoSuchFileException;
import com.parquet.engine.query.source.ParquetSource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class LocalParquetSource implements ParquetSource {

  private final ParquetProperties props;

  @Override
  public Path getParquetPath(String fileName) {

    if (fileName == null || fileName.isBlank()) {
      throw new IllegalArgumentException("Parquet file name must not be null or blank");
    }

    String basePath = props.getLocal().getInputDir();
    if (basePath == null || basePath.isBlank()) {
      throw new IllegalStateException("Local parquet files path is not configured");
    }

    try {
      Path parquetPath = Path.of(basePath, fileName).normalize();

      if (!Files.exists(parquetPath)) {
        throw new NoSuchFileException("Parquet file not found: " + parquetPath);
      }

      if (!Files.isRegularFile(parquetPath)) {
        throw new IllegalStateException("Parquet path is not a file: " + parquetPath);
      }

      return parquetPath;

    } catch (InvalidPathException ex) {
      throw new IllegalArgumentException("Invalid parquet file path: " + fileName, ex);
    }
  }

  @Override
  public List<String> listFileNames() {
    String dirName = props.getLocal().getInputDir();
    if (dirName == null || dirName.isBlank()) {
      return List.of();
    }

    Path directory = Path.of(dirName);

    if (Files.notExists(directory) || !Files.isDirectory(directory)) {
      return List.of();
    }

    try (Stream<Path> paths = Files.list(directory)) {
      return paths.filter(Files::isRegularFile).map(Path::getFileName).map(Path::toString)
          .filter(name -> name.toLowerCase().endsWith(".parquet")).toList();
    } catch (IOException e) {
      throw new RuntimeException("Failed to list parquet files in directory: " + dirName, e);
    }
  }

  @Override
  public boolean moveFileToProcessedDir(String fileName) {

    if (fileName == null || fileName.isBlank()) {
      return false;
    }

    Path inputDir = Path.of(props.getLocal().getInputDir());
    Path processedDir = Path.of(props.getLocal().getProcessedDir());

    Path source = inputDir.resolve(fileName);
    Path destination = processedDir.resolve(fileName);

    try {
      if (Files.notExists(source) || !Files.isRegularFile(source)) {
        return false;
      }

      // Ensuring processed directory exists
      Files.createDirectories(processedDir);

      // Atomic moving if supported by FS
      Files.move(source, destination, StandardCopyOption.REPLACE_EXISTING,
          StandardCopyOption.ATOMIC_MOVE);

      return true;

    } catch (IOException e) {
      log.error("Failed to move file {} to processed dir", fileName, e);
      return false;
    }
  }

  @Override
  public boolean moveFileToFailedDir(String fileName) {

    if (fileName == null || fileName.isBlank()) {
      return false;
    }

    Path inputDir = Path.of(props.getLocal().getInputDir());
    Path failedDir = Path.of(props.getLocal().getFailedDir());

    Path source = inputDir.resolve(fileName);
    Path destination = failedDir.resolve(fileName);

    try {
      if (Files.notExists(source) || !Files.isRegularFile(source)) {
        return false;
      }

      Files.createDirectories(failedDir);

      Files.move(source, destination, StandardCopyOption.REPLACE_EXISTING,
          StandardCopyOption.ATOMIC_MOVE);

      return true;

    } catch (IOException e) {
      log.error("Failed to move file {} to failed dir", fileName, e);
      return false;
    }
  }


}
