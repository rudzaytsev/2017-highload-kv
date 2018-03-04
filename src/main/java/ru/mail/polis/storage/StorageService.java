package ru.mail.polis.storage;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVService;
import ru.mail.polis.storage.interaction.*;
import ru.mail.polis.utils.QueryParams;
import ru.mail.polis.utils.Replicas;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.net.InetSocketAddress;
import java.util.NoSuchElementException;
import java.util.Set;

import static java.lang.Integer.parseInt;
import static ru.mail.polis.utils.QueryParams.*;

/**
 * Implements key-value storage main logic
 */
public class StorageService implements KVService {

  @NotNull
  private final HttpServer server;

  @NotNull
  private final Set<String> topology;

  @NotNull
  private final DAO dao;

  public StorageService(@NotNull DAO dao, @NotNull Set<String> topology, int port) throws IOException {
    this.server = HttpServer.create(new InetSocketAddress(port), 0);
    this.topology = topology;
    this.dao = dao;
    initMapping();
  }

  private void initMapping() {
    server.createContext("/v0/status", this::status);
    server.createContext("/v0/entity", this::entity);
  }

  private void entity(HttpExchange exchange) throws IOException, NotEnoughReplicasSentAcknowledge {
    new ErrorHandler(this::crudEntity).handle(exchange);
  }

  private void crudEntity(HttpExchange http) throws IOException, NotEnoughReplicasSentAcknowledge {
    String query = http.getRequestURI().getQuery();
    QueryParams params = extractQuery(query);
    String httpMethod = http.getRequestMethod();
    if ("GET".equals(httpMethod)) {
      readData(http, params.id, params.replicas);
    }
    else if ("PUT".equals(httpMethod)) {
      upsertData(http, params);
    }
    else if ("DELETE".equals(httpMethod)) {
      deleteData(http, params);
    }
    else {
      http.sendResponseHeaders(405, 0);
    }
  }

  private void deleteData(HttpExchange http, QueryParams params) throws IOException {
    Replicas replicas = params.replicas;
    if (replicasExist(replicas) && !topology.isEmpty()) {
      deleteDataFromNodes(params.id, replicas.otherReplicas());
    }
    dao.delete(params.id);
    http.sendResponseHeaders(202, 0);
  }

  private void deleteDataFromNodes(String id, Replicas replicas) {
    DeleteDataFromCluster.with(id, replicas, getCurrentNodeAddress(), topology).run();
  }

  private void upsertData(HttpExchange http, QueryParams params) throws IOException {
    byte[] data = getData(params.id, http);
    Replicas replicas = params.replicas;
    if (replicasExist(replicas) && !topology.isEmpty()) {
      upsertDataToAnotherNodes(params.id, data, replicas.otherReplicas());
    }
    dao.upsert(params.id, data);
    http.sendResponseHeaders(201, 0);
  }

  private void upsertDataToAnotherNodes(String id, byte[] data, Replicas replicas) {
    UpsertDataOnCluster.with(id, data, replicas, getCurrentNodeAddress(), topology).run();
  }

  private void readData(HttpExchange http, String id, Replicas replicas) throws IOException, NotEnoughReplicasSentAcknowledge {

    if (replicasExist(replicas) && !topology.isEmpty()) {
      readDataFromAnotherNodes(id, replicas.otherReplicas());
    }
    byte[] data = dao.get(id);
    http.sendResponseHeaders(200, data.length);
    try (OutputStream out = http.getResponseBody()) {
      out.write(data);
    }
  }

  private void readDataFromAnotherNodes(String id, Replicas replicas) throws NotEnoughReplicasSentAcknowledge {
    ReadDataFromCluster.with(id, replicas, getCurrentNodeAddress(), topology).run();
  }

  private String getCurrentNodeAddress() {
    InetSocketAddress address = server.getAddress();
    return "http://localhost" + ":" + address.getPort();
  }



  /**
   * Checks replicas values
   *
   * @param replicas as pair ack and from
   * @return true if replicas are set properly i.e ack > 0 and ack <= from,
   *         false if replicas parameter is null, otherwise an exception will be thrown
   * @throws IllegalArgumentException if replicas are not properly set
   */
  private boolean replicasExist(Replicas replicas) {
    if (replicas == null)
      return false;

    if (replicas.ack <= 0) {
      throw new IllegalArgumentException("Ack value should be positive");
    }
    if (replicas.ack > replicas.from) {
      throw new IllegalArgumentException("Ack value should be less or equal than From value");
    }
    return true;
  }

  private static class ErrorHandler implements HttpHandler {

    private final HttpHandler delegate;

    public ErrorHandler(HttpHandler delegate) {
      this.delegate = delegate;
    }

    @Override
    public void handle(HttpExchange http) throws IOException {
      try {
        delegate.handle(http);
      }
      catch (IllegalArgumentException e) {
        sendError(400, http, e);
      }
      catch (NoSuchElementException e) {
        sendError(404, http, e);
      }
      catch (IOException e) {
        sendError(500, http, e);
      }
      catch (NotEnoughReplicasSentAcknowledge e) {
        sendError(504, http, e);
      }
      finally {
        http.close();
      }
    }

    private void sendError(int statusCode, HttpExchange http, Exception e) throws IOException {
      String errorMsg = e.getMessage();
      http.sendResponseHeaders(statusCode, errorMsg.length());
      try(OutputStream out = http.getResponseBody()){
        out.write(errorMsg.getBytes());
      }
    }
  }

  private byte[] getData(String id, HttpExchange http) throws IOException {
    final int contentLength = parseInt(http.getRequestHeaders().getFirst("Content-Length"));
    if (contentLength == 0)
      return new byte[]{};

    byte[] data = new byte[contentLength];
    try(InputStream in = http.getRequestBody()) {
      if (in.read(data) != contentLength) {
        throw new IOException("Can't read request body");
      }
    }
    return data;
  }

  private void status(HttpExchange http) throws IOException {
    if ("GET".equals(http.getRequestMethod())) {
      String response = "ONLINE";
      http.sendResponseHeaders(200, response.length());
      try(OutputStream out = http.getResponseBody()) {
        out.write(response.getBytes());
      }
    }
  }

  @Override
  public void start() {
    server.start();
  }

  @Override
  public void stop() {
    server.stop(0);
  }
}
