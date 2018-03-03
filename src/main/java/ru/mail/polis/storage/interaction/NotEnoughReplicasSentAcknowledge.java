package ru.mail.polis.storage.interaction;

/**
* Created by rudolph on 03.03.18.
*/
public class NotEnoughReplicasSentAcknowledge extends RuntimeException {
  public NotEnoughReplicasSentAcknowledge(String message) {
    super(message);
  }
}
