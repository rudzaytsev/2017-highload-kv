package ru.mail.polis.storage.interaction;


import org.apache.http.HttpResponse;


/**
 * Interface for different interactions with nodes of cluster
 */
public interface ClusterInteraction {

  public HttpResponse run() throws NotEnoughReplicasSentAcknowledge;

  public boolean wellDone(int statusCode);

  public String httpMethod();

}
