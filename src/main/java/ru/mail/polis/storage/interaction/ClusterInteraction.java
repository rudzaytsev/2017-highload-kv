package ru.mail.polis.storage.interaction;


/**
 * Interface for different interactions with nodes of cluster
 */
public interface ClusterInteraction {

  public void run() throws NotEnoughReplicasSentAcknowledge;

  public int wellDoneStatusCode();

  public String httpMethod();

}
