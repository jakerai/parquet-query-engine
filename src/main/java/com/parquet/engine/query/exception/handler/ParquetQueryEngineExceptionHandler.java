package com.parquet.engine.query.exception.handler;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.HttpMediaTypeNotSupportedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import com.parquet.engine.query.dto.response.ServiceResponse;
import com.parquet.engine.query.exception.NoSuchFileException;
import com.parquet.engine.query.exception.NotFound;

/**
 * @author Vishal Rai
 */

@RestControllerAdvice
public class ParquetQueryEngineExceptionHandler {

  @ExceptionHandler(MethodArgumentNotValidException.class)
  public ResponseEntity<ServiceResponse<Map<String, String>>> handleValidationErrors(
      MethodArgumentNotValidException ex) {
    Map<String, String> errors = new HashMap<>();
    ex.getBindingResult().getFieldErrors()
        .forEach(error -> errors.put(error.getField(), error.getDefaultMessage()));

    ServiceResponse<Map<String, String>> response = ServiceResponse.<Map<String, String>>builder()
        .success(false).status(HttpStatus.BAD_REQUEST.value()).message("Validation Failed")
        .errors(errors).timestamp(Instant.now()).build();

    return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
  }

  @ExceptionHandler(HttpMediaTypeNotSupportedException.class)
  public ResponseEntity<ServiceResponse<Void>> handleHttpMediaTypeNotSupported(
      HttpMediaTypeNotSupportedException ex) {
    StringBuilder supportedTypes = new StringBuilder();
    ex.getSupportedMediaTypes().forEach(t -> supportedTypes.append(t.toString()).append(", "));
    String supported =
        supportedTypes.length() > 0 ? supportedTypes.substring(0, supportedTypes.length() - 2)
            : "None";

    return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(ServiceResponse.error(
        "Unsupported Content-Type. Supported types: " + supported, HttpStatus.BAD_REQUEST.value()));
  }

  @ExceptionHandler(NoSuchFileException.class)
  public ResponseEntity<ServiceResponse<Void>> handleNoSuchFileException(NoSuchFileException ex) {
    return ResponseEntity.status(HttpStatus.NOT_FOUND)
        .body(ServiceResponse.error(ex.getMessage(), HttpStatus.NOT_FOUND.value()));
  }

  @ExceptionHandler(NotFound.class)
  public ResponseEntity<ServiceResponse<Void>> handleNotFound(NotFound ex) {
    return ResponseEntity.status(HttpStatus.NOT_FOUND)
        .body(ServiceResponse.error(ex.getMessage(), HttpStatus.NOT_FOUND.value()));
  }
}
