package ru.mail.polis.storage;

/**
 * Created by rudolph on 30.04.18.
 */
public class DataDeletedException extends RuntimeException {

  private String tombstone;

  public DataDeletedException(String message, String tombstone) {
    super(message);
    this.tombstone = tombstone;
  }

  public String getTombstone() {
    return tombstone;
  }
}
