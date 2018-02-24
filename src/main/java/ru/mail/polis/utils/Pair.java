package ru.mail.polis.utils;

/**
 * Created by rudolph on 24.02.18.
 */
public class Pair<F, S> {
  public final F _1;
  public final S _2;

  public Pair(F _1, S _2) {
    this._1 = _1;
    this._2 = _2;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Pair pair = (Pair) o;

    if (!_1.equals(pair._1)) return false;
    if (!_2.equals(pair._2)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = _1.hashCode();
    result = 31 * result + _2.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "Pair{" +
      "_1=" + _1 +
      ", _2=" + _2 +
      '}';
  }
}
