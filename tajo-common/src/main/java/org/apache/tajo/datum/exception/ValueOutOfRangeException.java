package org.apache.tajo.datum.exception;

public class ValueOutOfRangeException extends RuntimeException {
  public ValueOutOfRangeException(String message) {
    super(message);
  }
}
