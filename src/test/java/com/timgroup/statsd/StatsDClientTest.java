package com.timgroup.statsd;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Test;

public class StatsDClientTest {

    private static final int STATSD_SERVER_PORT = 17254;
    private final StatsDClient _client = new StatsDClient("my.prefix", "localhost", STATSD_SERVER_PORT);
    private final StatsDClient _noPrefixClient = new StatsDClient("localhost", STATSD_SERVER_PORT);

    @After
    public void stop() throws Exception {
        _client.stop();
    }

    @Test(timeout=5000L)
    public void testCounter() throws Exception {
        counter(_client, true);
        counter(_noPrefixClient, false);
    }

    private void counter(StatsDClient client, boolean hasPrefix) throws Exception {
        final DummyStatsDServer server = new DummyStatsDServer(STATSD_SERVER_PORT);
        client.count("mycount", 24);
        server.waitForMessage();
        if (hasPrefix) {
            assertThat(server.messagesReceived(), contains("my.prefix.mycount:24|c"));
        } else {
            assertThat(server.messagesReceived(), contains("mycount:24|c"));
        }
    }

    @Test(timeout=5000L)
    public void testCounterIncrement() throws Exception {
        counterIncrement(_client, true);
        counterIncrement(_noPrefixClient, false);
    }

    private void counterIncrement(StatsDClient client, boolean hasPrefix) throws Exception {
        final DummyStatsDServer server = new DummyStatsDServer(STATSD_SERVER_PORT);
        client.incrementCounter("myinc");
        server.waitForMessage();
        if (hasPrefix) {
            assertThat(server.messagesReceived(), contains("my.prefix.myinc:1|c"));
        } else {
            assertThat(server.messagesReceived(), contains("myinc:1|c"));
        }
    }

    @Test(timeout=5000L)
    public void testCounterDecrement() throws Exception {
        counterDecrement(_client, true);
        counterDecrement(_noPrefixClient, false);
    }

    private void counterDecrement(StatsDClient client, boolean hasPrefix) throws Exception {
        final DummyStatsDServer server = new DummyStatsDServer(STATSD_SERVER_PORT);
        client.decrement("mydec");
        server.waitForMessage();
        if (hasPrefix) {
            assertThat(server.messagesReceived(), contains("my.prefix.mydec:-1|c"));
        } else {
            assertThat(server.messagesReceived(), contains("mydec:-1|c"));
        }
    }

    @Test(timeout=5000L)
    public void testGuage() throws Exception {
        guage(_client, true);
        guage(_noPrefixClient, false);
    }

    private void guage(StatsDClient client, boolean hasPrefix) throws Exception {
        final DummyStatsDServer server = new DummyStatsDServer(STATSD_SERVER_PORT);
        client.recordGaugeValue("mygauge", 423);
        server.waitForMessage();
        if (hasPrefix) {
            assertThat(server.messagesReceived(), contains("my.prefix.mygauge:423|g"));
        } else {
            assertThat(server.messagesReceived(), contains("mygauge:423|g"));
        }
    }

    @Test(timeout=5000L)
    public void testTimer() throws Exception {
        timer(_client, true);
        timer(_noPrefixClient, false);
    }

    public void timer(StatsDClient client, boolean hasPrefix) throws Exception {
        final DummyStatsDServer server = new DummyStatsDServer(STATSD_SERVER_PORT);
        client.recordExecutionTime("mytime", 123);
        server.waitForMessage();
        if (hasPrefix) {
            assertThat(server.messagesReceived(), contains("my.prefix.mytime:123|ms"));
        } else {
            assertThat(server.messagesReceived(), contains("mytime:123|ms"));
        }
    }

    private static final class DummyStatsDServer {
        private final List<String> messagesReceived = new ArrayList<String>();
        private final DatagramSocket server;

        public DummyStatsDServer(int port) throws SocketException {
            server = new DatagramSocket(port);
            new Thread(new Runnable() {
                @Override public void run() {
                    try {
                        final DatagramPacket packet = new DatagramPacket(new byte[256], 256);
                        server.receive(packet);
                        messagesReceived.add(new String(packet.getData()).trim());
                        server.close();
                    } catch (Exception e) { }
                }
            }).start();
        }

        public void waitForMessage() {
            while (messagesReceived.isEmpty()) {
                try {
                    Thread.sleep(50L);
                } catch (InterruptedException e) {}}
        }

        public List<String> messagesReceived() {
            return new ArrayList<String>(messagesReceived);
        }
    }
}