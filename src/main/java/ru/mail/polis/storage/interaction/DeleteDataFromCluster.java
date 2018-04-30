package ru.mail.polis.storage.interaction;

import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Request;
import ru.mail.polis.utils.Replicas;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Represents delete data from cluster operation
 */
public class DeleteDataFromCluster extends AbstractClusterInteraction {

  private String id;

  private DeleteDataFromCluster(String id, Replicas replicas, Set<String> topology) {
    super(replicas, topology);
    this.id = id;
  }

  @Override
  protected HttpResponse selectResponseFromCluster(List<HttpResponse> responses) {
    return responses.get(0);
  }

  @Override
  protected HttpResponse makeRequest(String nodeUrl) throws IOException {
    return Request.Delete(internalUrl(nodeUrl, id)).execute().returnResponse();
  }


  @Override
  public boolean wellDone(int statusCode) {
    return statusCode == 202;
  }

  @Override
  public String httpMethod() {
    return "DELETE";
  }

  public static ClusterInteraction with(String id, Replicas replicas, Set<String> topology) {
    return new DeleteDataFromCluster(id, replicas, topology);
  }

}
