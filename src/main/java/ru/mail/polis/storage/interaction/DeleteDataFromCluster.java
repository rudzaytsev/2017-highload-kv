package ru.mail.polis.storage.interaction;

import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Request;

import java.io.IOException;
import java.util.Set;

/**
 * Represents delete data from cluster operation
 */
public class DeleteDataFromCluster extends AbstractClusterInteraction {

  private String id;

  private DeleteDataFromCluster(String id, int ack, int from, String currentNodeAddress, Set<String> topology) {
    super(ack, from, currentNodeAddress, topology);
    this.id = id;
  }

  @Override
  protected HttpResponse makeRequest(String nodeUrl) throws IOException {
    return Request.Delete(entityUrl(nodeUrl, id)).execute().returnResponse();
  }

  @Override
  public int wellDoneStatusCode() {
    return 202;
  }

  @Override
  public String httpMethod() {
    return "DELETE";
  }

  public static ClusterInteraction with(String id, int ack, int from, String currentNodeAddress, Set<String> topology) {
    return new DeleteDataFromCluster(id, ack, from, currentNodeAddress, topology);
  }

}
