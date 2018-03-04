package ru.mail.polis.storage.interaction;

import org.apache.http.HttpResponse;
import ru.mail.polis.utils.Replicas;

import java.io.IOException;
import java.util.Set;

import static java.lang.String.format;

/**
 * This class contains implementation of universal interaction logic with cluster.
 * Some specific steps of this algorithm should be overridden by inheritor-classes
 */
public abstract class AbstractClusterInteraction implements ClusterInteraction {

  private Replicas replicas;
  private String currentNodeAddress;
  private Set<String> topology;

  public AbstractClusterInteraction(Replicas replicas, String currentNodeAddress, Set<String> topology) {
    this.replicas = replicas;
    this.currentNodeAddress = currentNodeAddress;
    this.topology = topology;
  }

  @Override
  public void run() throws NotEnoughReplicasSentAcknowledge {
    if (replicas.ack <= 0) return;

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

      if (anotherNodesAcks == replicas.from) {
        return;
      }

    }
    throw new NotEnoughReplicasSentAcknowledge(
      format("For %s command  should response %d/%d but was %d/%d",
             httpMethod(), replicas.ack + 1, replicas.from + 1, anotherNodesAcks + 1, replicas.from + 1)
    );

  }

  protected abstract HttpResponse makeRequest(String nodeUrl) throws IOException;

  String entityUrl(String nodeUrl, String id) {
    return nodeUrl + "/v0/entity?id=" + id;
  }



}
