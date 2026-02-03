package com.parquet.engine.query.dto.request;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ServiceRequest<T> {

  @NotNull(message = "Payload cannot be null")
  @Valid
  private T payload;

  public static <T> ServiceRequest<T> of(T payload) {
    return new ServiceRequest<>(payload);
  }

}
