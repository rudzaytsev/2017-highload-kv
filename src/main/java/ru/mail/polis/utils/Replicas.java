package ru.mail.polis.utils;

/**
 * Represent replicas params
 */
public final class Replicas {

  public final int ack;
  public final int from;

  public Replicas(int ack, int from) {
    this.ack = ack;
    this.from = from;
  }

  public Replicas otherReplicas() {
    return new Replicas(ack - 1, from - 1);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Replicas replicas = (Replicas) o;

    if (ack != replicas.ack) return false;

    return from == replicas.from;
  }

  @Override
  public int hashCode() {
    int result = ack;
    result = 31 * result + from;
    return result;
  }

  @Override
  public String toString() {
    return "Replicas{" + ack + "/" + from + "}";
  }
}
