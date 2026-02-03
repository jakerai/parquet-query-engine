package com.parquet.engine.query.dto.response;

import java.time.Instant;
import io.opentelemetry.api.trace.Span;
import lombok.Builder;

@Builder
public record ServiceResponse<T>(boolean success, String message, int status, Object errors,
    Instant timestamp, String requestId, T data) {


  /* SUCCESS */
  public static <T> ServiceResponse<T> success(T data, String message) {
    return new ServiceResponse<>(true, message, 0, null, Instant.now(), getTraceId(),
        data);
  }

  public static <T> ServiceResponse<T> success(String message) {
    return success(null, message);
  }

  /* ERROR */

  public static <T> ServiceResponse<T> error(String message, Object errors) {
    return new ServiceResponse<>(false, message, -1, errors, Instant.now(),
        getTraceId(), null);
  }

  public static <T> ServiceResponse<T> error(String message) {
    return error(message, null);
  }

  private static String getTraceId() {
    Span currentSpan = Span.current();
    return (currentSpan != null && currentSpan.getSpanContext().isValid())
        ? currentSpan.getSpanContext().getTraceId()
        : "no-trace-id";
  }
  
}
