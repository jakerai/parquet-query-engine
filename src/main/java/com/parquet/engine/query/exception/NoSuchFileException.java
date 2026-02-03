package com.parquet.engine.query.exception;

public class NoSuchFileException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public NoSuchFileException(String message) {
    super(message);
  }

}
