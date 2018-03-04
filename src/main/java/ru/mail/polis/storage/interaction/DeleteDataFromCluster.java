package ru.mail.polis.storage.interaction;

import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Request;
import ru.mail.polis.utils.Replicas;

import java.io.IOException;
import java.util.Set;

/**
 * Represents delete data from cluster operation
 */
public class DeleteDataFromCluster extends AbstractClusterInteraction {

  private String id;

  private DeleteDataFromCluster(String id, Replicas replicas, String currentNodeAddress, Set<String> topology) {
    super(replicas, currentNodeAddress, topology);
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

  public static ClusterInteraction with(String id, Replicas replicas, String currentNodeAddress, Set<String> topology) {
    return new DeleteDataFromCluster(id, replicas, currentNodeAddress, topology);
  }

}
