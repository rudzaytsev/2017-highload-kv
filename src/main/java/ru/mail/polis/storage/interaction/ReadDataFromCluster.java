package ru.mail.polis.storage.interaction;

import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Request;
import ru.mail.polis.storage.Tombstone;
import ru.mail.polis.utils.Replicas;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

/**
 * Represents read data from cluster operation
 */
public class ReadDataFromCluster extends AbstractClusterInteraction {

  private String id;
  private List<Integer> availableStatusCodes = Arrays.asList(200, 404);

  private Tombstone tombstone = new Tombstone();

  private ReadDataFromCluster(String id, Replicas replicas, Set<String> topology) {
    super(replicas, topology);
    this.id = id;
  }

  @Override
  protected HttpResponse selectResponseFromCluster(List<HttpResponse> responses) {
    boolean noAckReplicasHasContent = responses.stream().allMatch(this::hasEmptyContent);
    boolean dataDeletedOnSomeReplicas = responses.stream().anyMatch(this::hasDeletedData);

    Optional<HttpResponse> optResponse = responses.stream().filter(this::notFound).findFirst();
    if (noAckReplicasHasContent || dataDeletedOnSomeReplicas) {
      return optResponse.get();
    }
    return responses.stream().filter(this::ok).findFirst().orElse(responses.get(0));
  }


  private boolean hasEmptyContent(HttpResponse response) {
    return response.getEntity().getContentLength() == 0;
  }

  private boolean hasDeletedData(HttpResponse response) {
    return tombstone.isSetIn(getResponseContent(response));
  }

  private boolean notFound(HttpResponse response) {
    return response.getStatusLine().getStatusCode() == 404;
  }

  private boolean ok(HttpResponse response) {
    return response.getStatusLine().getStatusCode() == 200;
  }

  private byte[] getResponseContent(HttpResponse response) {
    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    try {
      response.getEntity().writeTo(byteArrayOutputStream);
    } catch (IOException e) {
      return new byte[]{};
    }
    return byteArrayOutputStream.toByteArray();
  }

  @Override
  protected HttpResponse makeRequest(String nodeUrl) throws IOException {
    return Request.Get(internalUrl(nodeUrl, id)).execute().returnResponse();
  }

  @Override
  public boolean wellDone(int statusCode) {
    return availableStatusCodes.contains(statusCode);
  }

  @Override
  public String httpMethod() {
    return "GET";
  }

  public static ClusterInteraction with(String id, Replicas replicas, Set<String> topology) {
    return new ReadDataFromCluster(id, replicas, topology);
  }

}
