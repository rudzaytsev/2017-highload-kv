package ru.mail.polis.storage;

/**
 * Created by rudolph on 30.04.18.
 */
public class IdNotFoundException extends RuntimeException {
  public IdNotFoundException(String message) {
    super(message);
  }
}
