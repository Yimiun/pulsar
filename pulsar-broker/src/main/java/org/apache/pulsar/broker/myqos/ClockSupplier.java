package org.apache.pulsar.broker.myqos;

public interface ClockSupplier {

    long currentTimeMillis();
}
