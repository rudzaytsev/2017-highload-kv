package ru.mail.polis.storage.interaction;


import java.util.Set;

/**
 * Interface for different interactions with nodes of cluster
 */
public interface ClusterInteraction {

  public void run() throws NotEnoughReplicasSentAcknowledge;

  public boolean wellDone(int statusCode);

  public String httpMethod();

}
