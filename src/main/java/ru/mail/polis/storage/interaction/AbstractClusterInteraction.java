package ru.mail.polis.storage.interaction;

import org.apache.http.HttpResponse;
import ru.mail.polis.utils.Replicas;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static java.lang.String.format;

/**
 * This class contains implementation of universal interaction logic with cluster.
 * Some specific steps of this algorithm should be overridden by inheritor-classes
 */
public abstract class AbstractClusterInteraction implements ClusterInteraction {

  private Replicas replicas;
  private Set<String> topology;

  public AbstractClusterInteraction(Replicas replicas, Set<String> topology) {
    this.replicas = replicas;
    this.topology = topology;
  }

  @Override
  public HttpResponse run() throws NotEnoughReplicasSentAcknowledge {
    if (replicas.from <= 0) return null;

    List<HttpResponse> succeedResponses = new ArrayList<>();
    int totalInternalRequests = 0;
    for (String nodeUrl : topology) {

      try {
        HttpResponse response = makeRequest(nodeUrl);
        int statusCode = response.getStatusLine().getStatusCode();
        if (wellDone(statusCode)) {
          succeedResponses.add(response);
        }
      }
      catch (IOException e) {
        e.printStackTrace();
      }
      finally {
        totalInternalRequests++;
      }

      if (succeedResponses.size() == replicas.from) {
        return selectResponseFromCluster(succeedResponses);
      }
      if (totalInternalRequests == replicas.from) break;
    }

    if (succeedResponses.size() < replicas.ack) {
      throw new NotEnoughReplicasSentAcknowledge(
        format("For %s command  should response %d/%d but was %d/%d",
          httpMethod(), replicas.ack, replicas.from, succeedResponses.size(), replicas.from)
      );
    }
    return selectResponseFromCluster(succeedResponses);
  }

  protected abstract HttpResponse selectResponseFromCluster(List<HttpResponse> responses);


  protected abstract HttpResponse makeRequest(String nodeUrl) throws IOException;

  String internalUrl(String nodeUrl, String id) {
    return nodeUrl + "/v0/internal?id=" + id;
  }



}
