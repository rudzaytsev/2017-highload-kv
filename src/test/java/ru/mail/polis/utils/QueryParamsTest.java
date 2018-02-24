package ru.mail.polis.utils;

import org.junit.Test;

import static org.junit.Assert.*;
import static ru.mail.polis.utils.QueryParams.*;

public class QueryParamsTest {

  @Test
  public void extractIdFromQueryWithSingleParameter() throws Exception {
    assertEquals("77", extractQuery("id=77").id);
  }

  @Test(expected = IllegalArgumentException.class)
  public void cannotExtractAbsentId() throws Exception {
    extractQuery("id=");
  }

  @Test(expected = IllegalArgumentException.class)
  public void cannotExtractIdIfQueryIsNotSet() throws Exception {
    extractQuery(null);
  }

  @Test
  public void extractReplicasFromQueryWithIdParameter() throws Exception {
    assertNull(extractQuery("id=77").replicas);
  }

  @Test
  public void extractIdFromQueryWithManyParameters() {
    assertEquals("77", extractQuery("id=77&replicas=3/2").id);
  }

  @Test
  public void extractReplicasFromQueryWithManyParameters() throws Exception {
    Pair<Integer, Integer> expected = new Pair<>(3,2);
    assertEquals(expected, extractQuery("id=77&replicas=3/2").replicas);
  }

  @Test(expected = IllegalArgumentException.class)
  public void replicasShouldHaveAckParameter() {
    extractQuery("id=77&replicas=/2");
  }

  @Test(expected = IllegalArgumentException.class)
  public void replicasShouldHaveFromParameter() throws Exception {
    extractQuery("id=77&replicas=3/");
  }

  @Test(expected = IllegalArgumentException.class)
  public void replicasValuesShouldNotBeAbsent() throws Exception {
    extractQuery("id=77&replicas=");
  }
}