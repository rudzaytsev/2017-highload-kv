package ru.mail.polis.storage;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.mail.polis.KVService;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.NoSuchElementException;

import static java.lang.Integer.parseInt;

/**
 * Created by rudolph on 04.02.18.
 */
public class StorageService implements KVService {

  public static final String ID_PREFIX = "=id";
  @NotNull
  private final HttpServer server;

  @NotNull
  private final DAO dao;

  public StorageService(@NotNull DAO dao, int port) throws IOException {
    this.server = HttpServer.create(new InetSocketAddress(port), 0);
    this.dao = dao;
    initMapping();
  }

  private void initMapping() {
    server.createContext("/v0/status", this::status);
    server.createContext("/v0/entity", this::entity);
  }

  private void entity(HttpExchange exchange) throws IOException {
    new ErrorHandler(this::crudEntity).handle(exchange);
  }

  private void crudEntity(HttpExchange http) throws IOException {
    String id = extractId(http.getRequestURI().getQuery());
    String httpMethod = http.getRequestMethod();
    if ("GET".equals(httpMethod)) {
      byte[] data = dao.get(id);
      http.sendResponseHeaders(200, data.length);
      try (OutputStream out = http.getResponseBody()) {
        out.write(data);
      }
    }
    else if ("PUT".equals(httpMethod)) {
      dao.upsert(id, getData(id, http));
      http.sendResponseHeaders(201, 0);
    }
    else if ("DELETE".equals(httpMethod)) {
      dao.delete(id);
      http.sendResponseHeaders(202, 0);
    }
    else {
      http.sendResponseHeaders(405, 0);
    }
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

  private String extractId(@Nullable String query) {
    if (query == null)
      throw new IllegalArgumentException("Request should contains query string");

    if (query.startsWith(ID_PREFIX))
      throw new IllegalArgumentException("Incorrect query string. Should starts with id=");

    String id = query.substring(ID_PREFIX.length());

    if (id.isEmpty())
      throw new IllegalArgumentException("Id should be non empty");

    return id;
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
