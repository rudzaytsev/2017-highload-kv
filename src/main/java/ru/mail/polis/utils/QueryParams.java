package ru.mail.polis.utils;

import org.jetbrains.annotations.Nullable;

/**
 * Created by rudolph on 24.02.18.
 */
public class QueryParams {

  public static final String ID_PREFIX = "id=";
  public static final String REPLICAS_PREFIX = "replicas=";


  public final String id;
  public final Pair<Integer, Integer> replicas;


  private QueryParams(String id, Pair<Integer, Integer> replicas) {
    this.id = id;
    this.replicas = replicas;
  }

  public static QueryParams extractQuery(@Nullable String query) {
    if (query == null)
      throw new IllegalArgumentException("Request should contains query string");

    if (!query.startsWith(ID_PREFIX))
      throw new IllegalArgumentException("Incorrect query string. Should starts with id=");

    String[] parameters = query.split("&");
    if (parameters.length <= 0)
      throw new IllegalArgumentException("Query string should contains parameters");

    String id = parameters[0].substring(ID_PREFIX.length());

    if (id.isEmpty())
      throw new IllegalArgumentException("Id should be non empty");

    if (parameters.length < 2)
      return new QueryParams(id, null);

    String replicas = parameters[1].substring(REPLICAS_PREFIX.length());
    String[] ackFrom = replicas.split("/");
    if (ackFrom.length < 2)
      throw new IllegalArgumentException("Replicas parameter should have format ack/from");

    try {
      Integer ack = Integer.parseInt(ackFrom[0]);
      Integer from = Integer.parseInt(ackFrom[1]);
      return new QueryParams(id, new Pair<>(ack, from));
    }
    catch (NumberFormatException e) {
      throw new IllegalArgumentException("Elements of pair ack/from should have integer type", e);
    }
  }
 }
