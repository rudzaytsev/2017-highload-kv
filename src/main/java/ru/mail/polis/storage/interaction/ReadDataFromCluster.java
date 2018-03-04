package ru.mail.polis.storage.interaction;

import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Request;
import ru.mail.polis.utils.Replicas;

import java.io.IOException;
import java.util.Set;

/**
 * Represents read data from cluster operation
 */
public class ReadDataFromCluster extends AbstractClusterInteraction {

  private String id;

  private ReadDataFromCluster(String id, Replicas replicas, String currentNodeAddress, Set<String> topology) {
    super(replicas, currentNodeAddress, topology);
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

  public static ClusterInteraction with(String id, Replicas replicas, String currentNodeAddress, Set<String> topology) {
    return new ReadDataFromCluster(id, replicas, currentNodeAddress, topology);
  }

}
