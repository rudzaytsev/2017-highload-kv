package ru.mail.polis.storage;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.http.HttpResponse;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVService;
import ru.mail.polis.storage.interaction.*;
import ru.mail.polis.utils.QueryParams;
import ru.mail.polis.utils.Replicas;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.*;

import static java.lang.Integer.parseInt;
import static org.apache.http.HttpStatus.*;
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
    this.server = HttpServer.create(new InetSocketAddress(port), 5);
    server.setExecutor(new ThreadPoolExecutor(4, 10, 3, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(30)));
    this.topology = topology;
    this.dao = dao;
    initMapping();
  }

  private void initMapping() {
    server.createContext("/v0/status", this::status);
    server.createContext("/v0/entity", this::entity);
    server.createContext("/v0/internal", this::internal);
  }

  private void internal(HttpExchange exchange) throws IOException {
    new ErrorHandler(this::singleNodeRequest).handle(exchange);
  }

  private void singleNodeRequest(HttpExchange http) throws IOException {
    String query = http.getRequestURI().getQuery();
    QueryParams params = extractQuery(query);
    String httpMethod = http.getRequestMethod();
    if ("GET".equals(httpMethod)) {
      readDataFromNode(http, params.id);
    }
    else if ("PUT".equals(httpMethod)) {
      upsertDataFromNode(http, params);
    }
    else if ("DELETE".equals(httpMethod)) {
      deleteDataFromNode(http, params);
    }
    else {
      http.sendResponseHeaders(SC_METHOD_NOT_ALLOWED, 0);
    }

  }

  private void deleteDataFromNode(HttpExchange http, QueryParams params) throws IOException {
    dao.delete(params.id);
    http.sendResponseHeaders(SC_ACCEPTED, 0);
  }

  private void upsertDataFromNode(HttpExchange http, QueryParams params) throws IOException {
    byte[] data = getData(params.id, http);
    dao.upsert(params.id, data);
    http.sendResponseHeaders(SC_CREATED, 0);
  }

  private void readDataFromNode(HttpExchange http, String id) throws IOException {
    byte[] data = dao.get(id);
    http.sendResponseHeaders(SC_OK, data.length);
    try (OutputStream out = http.getResponseBody()) {
      out.write(data);
    }
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
      http.sendResponseHeaders(SC_METHOD_NOT_ALLOWED, 0);
    }
  }

  private void deleteData(HttpExchange http, QueryParams params) throws IOException {
    Replicas replicas = params.replicas;
    if (replicasExist(replicas) && !topology.isEmpty()) {
      deleteDataFromCluster(params.id, replicas);
      http.sendResponseHeaders(SC_ACCEPTED, 0);
    }
    else {
      deleteDataFromNode(http, params);
    }

  }

  private HttpResponse deleteDataFromCluster(String id, Replicas replicas) {
    return DeleteDataFromCluster.with(id, replicas, topology).run();
  }

  private void upsertData(HttpExchange http, QueryParams params) throws IOException {
    Replicas replicas = params.replicas;
    if (replicasExist(replicas) && !topology.isEmpty()) {
      byte[] data = getData(params.id, http);
      upsertDataToCluster(params.id, data, replicas);
      http.sendResponseHeaders(SC_CREATED, 0);
    }
    else {
      upsertDataFromNode(http, params);
    }
  }

  private HttpResponse upsertDataToCluster(String id, byte[] data, Replicas replicas) {
    return UpsertDataOnCluster.with(id, data, replicas, topology).run();
  }

  private void readData(HttpExchange http, String id, Replicas replicas) throws IOException, NotEnoughReplicasSentAcknowledge {
    if (replicasExist(replicas) && !topology.isEmpty()) {
      HttpResponse resp = readDataFromCluster(id, replicas);
      http.sendResponseHeaders(resp.getStatusLine().getStatusCode(), resp.getEntity().getContentLength());
      try (OutputStream out = http.getResponseBody()) {
        resp.getEntity().writeTo(out);
      }
    }
    else {
      readDataFromNode(http, id);
    }
  }

  private HttpResponse readDataFromCluster(String id, Replicas replicas) throws NotEnoughReplicasSentAcknowledge {
    return ReadDataFromCluster.with(id, replicas, topology).run();
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
        sendError(SC_BAD_REQUEST, http, e);
      }
      catch (DataDeletedException e) {
        sendError404(http, e);
      }
      catch (IdNotFoundException e) {
        sendError404(http, e);
      }
      catch (IOException e) {
        sendError(SC_INTERNAL_SERVER_ERROR, http, e);
      }
      catch (NotEnoughReplicasSentAcknowledge e) {
        sendError(SC_GATEWAY_TIMEOUT, http, e);
      }
      finally {
        http.close();
      }
    }

    private void sendError404(HttpExchange http, IdNotFoundException e) throws IOException {
      http.sendResponseHeaders(SC_NOT_FOUND, 0);
    }

    private void sendError404(HttpExchange http, DataDeletedException e) throws IOException {
      sendErrorResponse(SC_NOT_FOUND, http, e.getTombstone());
    }

    private void sendError(int statusCode, HttpExchange http, Exception e) throws IOException {
      sendErrorResponse(statusCode, http, e.getMessage());
    }

    private void sendErrorResponse(int statusCode, HttpExchange http, String errorMsg) throws IOException {
      http.sendResponseHeaders(statusCode, errorMsg.length());
      try (OutputStream out = http.getResponseBody()) {
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
      http.sendResponseHeaders(SC_OK, response.length());
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
