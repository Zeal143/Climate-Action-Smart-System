package com.climate.discovery;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.jmdns.JmDNS;
import javax.jmdns.ServiceEvent;
import javax.jmdns.ServiceInfo;
import javax.jmdns.ServiceListener;

/**
 * ClimateServiceDiscovery – JmDNS-based service discovery for the Climate Action system.
 *
 * Each GUI or client that needs to locate a gRPC service creates one instance of
 * this class, calls discoverService() to block until the service is found (or the
 * timeout fires), and then uses the returned host:port string to build a gRPC channel.
 *
 * Pattern mirrors ExampleServiceDiscovery from the course labs.
 */
public class ClimateServiceDiscovery {

    private static final Logger logger =
            Logger.getLogger(ClimateServiceDiscovery.class.getName());

    /** mDNS service type we are listening for, e.g. "_grpc._tcp.local." */
    private final String requiredServiceType;

    /** Service name substring to match, e.g. "AirQualityService" */
    private final String requiredServiceName;

    /** JmDNS multicast-DNS listener instance */
    private JmDNS jmdns;

    /**
     * Discovered host extracted from the resolved ServiceInfo.
     * Volatile so the ServiceListener callback (different thread) is visible here.
     */
    private volatile String discoveredHost;

    /** Discovered port extracted from the resolved ServiceInfo. */
    private volatile int discoveredPort;

    // -----------------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------------

    /**
     * @param serviceType mDNS service type, e.g. "_grpc._tcp.local."
     * @param serviceName Name (or name substring) to match during resolution
     */
    public ClimateServiceDiscovery(String serviceType, String serviceName) {
        this.requiredServiceType = serviceType;
        this.requiredServiceName = serviceName;
    }

    // -----------------------------------------------------------------------
    // Discovery
    // -----------------------------------------------------------------------

    /**
     * Blocks until the named service is resolved or the timeout expires.
     *
     * @param timeoutMilliseconds Maximum time to wait in milliseconds
     * @return "host:port" string for building a gRPC channel, or null on timeout
     * @throws InterruptedException if the waiting thread is interrupted
     * @throws IOException          if JmDNS cannot bind
     */
    public String discoverService(long timeoutMilliseconds)
            throws InterruptedException, IOException {

        CountDownLatch latch = new CountDownLatch(1);

        try {
            InetAddress localHost = InetAddress.getLocalHost();
            jmdns = JmDNS.create(localHost);
            logger.info("Discovery: JmDNS created on " + localHost
                    + " – looking for [" + requiredServiceName + "]");

            jmdns.addServiceListener(requiredServiceType, new ServiceListener() {

                @Override
                public void serviceAdded(ServiceEvent event) {
                    logger.info("Service added: " + event.getName());
                    // Request full resolution so serviceResolved fires
                    jmdns.requestServiceInfo(event.getType(), event.getName(), 1000);
                }

                @Override
                public void serviceRemoved(ServiceEvent event) {
                    logger.info("Service removed: " + event.getName());
                }

                @Override
                public void serviceResolved(ServiceEvent event) {
                    ServiceInfo info = event.getInfo();
                    String resolvedName = info.getName();
                    logger.info("Service resolved: " + resolvedName
                            + " @ port " + info.getPort());

                    // Only react if this is the service we are looking for
                    if (resolvedName.contains(requiredServiceName)) {
                        discoveredHost = info.getHostAddresses()[0];
                        discoveredPort = info.getPort();
                        logger.info("Matched! host=" + discoveredHost
                                + " port=" + discoveredPort);
                        latch.countDown();   // release the waiting thread
                    }
                }
            });

        } catch (UnknownHostException e) {
            logger.severe("Cannot determine local host: " + e.getMessage());
            throw e;
        }

        // Block until resolved or timeout
        boolean found = latch.await(timeoutMilliseconds, TimeUnit.MILLISECONDS);

        if (!found) {
            logger.warning("Discovery timed out waiting for [" + requiredServiceName + "]");
            return null;
        }

        String address = discoveredHost + ":" + discoveredPort;
        logger.info("discoverService returning: " + address);
        return address;
    }

    // -----------------------------------------------------------------------
    // Convenience getters (available after discoverService returns non-null)
    // -----------------------------------------------------------------------

    /** @return The discovered hostname or IP address */
    public String getDiscoveredHost() { return discoveredHost; }

    /** @return The discovered gRPC port */
    public int getDiscoveredPort()    { return discoveredPort; }

    // -----------------------------------------------------------------------
    // Shutdown
    // -----------------------------------------------------------------------

    /**
     * Closes the JmDNS socket and releases multicast.
     * Should be called when discovery is no longer needed.
     */
    public void close() throws IOException {
        if (jmdns != null) {
            jmdns.close();
            logger.info("ClimateServiceDiscovery closed for [" + requiredServiceName + "]");
        }
    }
}
