package ru.mail.polis.storage;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.fluent.Request;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVService;
import ru.mail.polis.utils.Pair;
import ru.mail.polis.utils.QueryParams;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.net.InetSocketAddress;
import java.util.NoSuchElementException;
import java.util.Set;

import static java.lang.Integer.parseInt;
import static java.lang.String.*;
import static ru.mail.polis.utils.QueryParams.*;

/**
 * Created by rudolph on 04.02.18.
 */
public class StorageService implements KVService {

  @NotNull
  private final HttpServer server;

  @NotNull
  private final Set<String> topology;

  @NotNull
  private final DAO dao;

  public StorageService(@NotNull DAO dao, Set<String> topology, int port) throws IOException {
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
    replicasExist(params.replicas);
    dao.delete(params.id);
    http.sendResponseHeaders(202, 0);
  }

  private void upsertData(HttpExchange http, QueryParams params) throws IOException {
    replicasExist(params.replicas);
    dao.upsert(params.id, getData(params.id, http));
    http.sendResponseHeaders(201, 0);
  }

  private void readData(HttpExchange http, String id, Pair<Integer, Integer> replicas) throws IOException, NotEnoughReplicasSentAcknowledge {

    if (replicasExist(replicas) && !topology.isEmpty()) {
      Integer ack = replicas._1;
      Integer from = replicas._2;
      readDataFromAnotherNodes(id, ack - 1, from - 1);
    }
    byte[] data = dao.get(id);
    http.sendResponseHeaders(200, data.length);
    try (OutputStream out = http.getResponseBody()) {
      out.write(data);
    }
  }

  private void readDataFromAnotherNodes(String id, Integer ack, Integer from) throws NotEnoughReplicasSentAcknowledge {
    if (ack == 0) return;

    InetSocketAddress address = server.getAddress();
    String currentNodeAddress = "http://localhost" + ":" + address.getPort();
    int anotherNodesAcks = 0;
    for (String nodeUrl : topology) {
      if (nodeUrl.equals(currentNodeAddress))
        continue;

      try {
        int statusCode = responseFromNode(nodeUrl, id).getStatusLine().getStatusCode();
        if (statusCode == 200) {
          anotherNodesAcks++;
        }
      }
      catch (IOException e) {
        e.printStackTrace();
      }

      if (anotherNodesAcks == from.intValue()) {
        return;
      }

    }
    throw new NotEnoughReplicasSentAcknowledge(
      format("For GET command  should response %d/%d but was %d/%d", ack + 1, from + 1, anotherNodesAcks + 1, from + 1)
    );

  }

  private static class NotEnoughReplicasSentAcknowledge extends RuntimeException {
    public NotEnoughReplicasSentAcknowledge(String message) {
      super(message);
    }
  }

  private HttpResponse responseFromNode(String nodeUrl, String id) throws IOException {
    return Request.Get(entityUrl(nodeUrl, id)).execute().returnResponse();
  }

  private String entityUrl(String nodeUrl, String id) {
    return nodeUrl + "/v0/entity?id=" + id;
  }

  /**
   * Checks replicas values
   *
   * @param replicas as pair ack and from
   * @return true if replicas are set properly i.e ack > 0 and ack <= from,
   *         false if replicas parameter is null, otherwise an exception will be thrown
   * @throws IllegalArgumentException if replicas are not properly set
   */
  private boolean replicasExist(Pair<Integer, Integer> replicas) {
    if (replicas == null)
      return false;

    Integer ack = replicas._1;
    Integer from = replicas._2;
    if (ack <= 0) {
      throw new IllegalArgumentException("Ack value should be positive");
    }
    if (ack > from) {
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
