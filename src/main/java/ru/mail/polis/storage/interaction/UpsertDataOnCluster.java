package ru.mail.polis.storage.interaction;

import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Request;
import ru.mail.polis.utils.Replicas;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.apache.http.HttpStatus.*;

/**
 * Represents insert or update data on cluster operation
 */
public class UpsertDataOnCluster extends AbstractClusterInteraction {

  private String id;
  private byte[] data;

  private UpsertDataOnCluster(String id, byte[] data, Replicas replicas, Set<String> topology) {
    super(replicas, topology);
    this.data = data;
    this.id = id;
  }

  @Override
  protected HttpResponse selectResponseFromCluster(List<HttpResponse> responses) {
    return responses.get(0);
  }

  @Override
  protected HttpResponse makeRequest(String nodeUrl) throws IOException {
    return Request.Put(internalUrl(nodeUrl, id)).bodyByteArray(data).execute().returnResponse();
  }

  @Override
  public boolean wellDone(int statusCode) {
    return statusCode == SC_CREATED;
  }

  @Override
  public String httpMethod() {
    return "PUT";
  }

  public static ClusterInteraction with(String id, byte[] data, Replicas replicas, Set<String> topology) {
    return new UpsertDataOnCluster(id, data, replicas, topology);
  }
}
