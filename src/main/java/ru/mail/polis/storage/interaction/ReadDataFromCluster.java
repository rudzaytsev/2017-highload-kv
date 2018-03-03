package ru.mail.polis.storage.interaction;

import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Request;

import java.io.IOException;
import java.util.Set;

/**
 * Represents read data from cluster operation
 */
public class ReadDataFromCluster extends AbstractClusterInteraction {

  private String id;

  private ReadDataFromCluster(String id, int ack, int from, String currentNodeAddress, Set<String> topology) {
    super(ack, from, currentNodeAddress, topology);
    this.id = id;
  }

  @Override
  protected HttpResponse makeRequest(String nodeUrl) throws IOException {
    return Request.Get(entityUrl(nodeUrl, id)).execute().returnResponse();
  }

  @Override
  public int wellDoneStatusCode() {
    return 200;
  }

  @Override
  public String httpMethod() {
    return "GET";
  }

  public static ClusterInteraction with(String id, int ack, int from, String currentNodeAddress, Set<String> topology) {
    return new ReadDataFromCluster(id, ack, from, currentNodeAddress, topology);
  }

}
