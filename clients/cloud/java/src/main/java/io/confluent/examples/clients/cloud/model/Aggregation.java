package io.confluent.examples.clients.cloud.model;

public class Aggregation {

    public Long total = 0l;

    public Aggregation updateFrom(Long count) {
      total += count;
      return this;
    }
}   