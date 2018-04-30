package ru.mail.polis.storage;

/**
 * Created by rudolph on 30.04.18.
 */
public class Tombstone {

  private static final String TOMBSTONE = "__DELETED__";

  public boolean isSetIn(byte[] data) {
    return TOMBSTONE.equals(new String(data));
  }

  public byte[] toBytes() {
    return TOMBSTONE.getBytes();
  }

  @Override
  public String toString() {
    return TOMBSTONE;
  }
}
