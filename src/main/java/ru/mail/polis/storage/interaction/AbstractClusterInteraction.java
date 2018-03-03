package ru.mail.polis.storage.interaction;

import org.apache.http.HttpResponse;

import java.io.IOException;
import java.util.Set;

import static java.lang.String.format;

/**
 * This class contains implementation of universal interaction logic with cluster.
 * Some specific steps of this algorithm should be overridden by inheritor-classes
 */
public abstract class AbstractClusterInteraction implements ClusterInteraction {

  private int ack;
  private int from;
  private String currentNodeAddress;
  private Set<String> topology;

  public AbstractClusterInteraction(int ack, int from, String currentNodeAddress, Set<String> topology) {
    this.ack = ack;
    this.from = from;
    this.currentNodeAddress = currentNodeAddress;
    this.topology = topology;
  }

  @Override
  public void run() throws NotEnoughReplicasSentAcknowledge {
    if (ack == 0) return;

    int anotherNodesAcks = 0;
    for (String nodeUrl : topology) {
      if (nodeUrl.equals(currentNodeAddress))
        continue;

      try {
        int statusCode = makeRequest(nodeUrl).getStatusLine().getStatusCode();
        if (statusCode == wellDoneStatusCode()) {
          anotherNodesAcks++;
        }
      }
      catch (IOException e) {
        e.printStackTrace();
      }

      if (anotherNodesAcks == from) {
        return;
      }

    }
    throw new NotEnoughReplicasSentAcknowledge(
      format("For %s command  should response %d/%d but was %d/%d", httpMethod(), ack + 1, from + 1, anotherNodesAcks + 1, from + 1)
    );

  }

  protected abstract HttpResponse makeRequest(String nodeUrl) throws IOException;

  String entityUrl(String nodeUrl, String id) {
    return nodeUrl + "/v0/entity?id=" + id;
  }



}
