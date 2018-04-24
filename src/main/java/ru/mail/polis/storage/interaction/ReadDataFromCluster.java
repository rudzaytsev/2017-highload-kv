package ru.mail.polis.storage.interaction;

import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Request;
import ru.mail.polis.utils.Replicas;

import java.io.IOException;
import java.util.*;

/**
 * Represents read data from cluster operation
 */
public class ReadDataFromCluster extends AbstractClusterInteraction {

  private String id;
  private List<Integer> availableStatusCodes = Arrays.asList(200, 404);

  private ReadDataFromCluster(String id, Replicas replicas, Set<String> topology) {
    super(replicas, topology);
    this.id = id;
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
