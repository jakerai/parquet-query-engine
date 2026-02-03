package com.parquet.engine.query.dto.request;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
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
public class IngestRequest {
    @NotBlank(message = "Parquet file name is required") 
    @Pattern(regexp = "^[\\w,\\s-]+\\.parquet$", message = "Invalid parquet file name")
    private String fileName;

}
