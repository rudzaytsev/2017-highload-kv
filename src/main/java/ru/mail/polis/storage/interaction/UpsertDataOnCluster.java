package ru.mail.polis.storage.interaction;

import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Request;

import java.io.IOException;
import java.util.Set;

/**
 * Represents insert or update data on cluster operation
 */
public class UpsertDataOnCluster extends AbstractClusterInteraction {

  private String id;
  private byte[] data;

  private UpsertDataOnCluster(String id, byte[] data, int ack, int from, String currentNodeAddress, Set<String> topology) {
    super(ack, from, currentNodeAddress, topology);
    this.data = data;
    this.id = id;
  }

  @Override
  protected HttpResponse makeRequest(String nodeUrl) throws IOException {
    return Request.Put(entityUrl(nodeUrl, id)).bodyByteArray(data).execute().returnResponse();
  }

  @Override
  public int wellDoneStatusCode() {
    return 201;
  }

  @Override
  public String httpMethod() {
    return "PUT";
  }

  public static ClusterInteraction with(String id, byte[] data, int ack, int from, String currentNodeAddress, Set<String> topology) {
    return new UpsertDataOnCluster(id, data, ack, from, currentNodeAddress, topology);
  }
}
