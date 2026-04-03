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

public class ClimateServiceDiscovery {

    private static final Logger logger
            = Logger.getLogger(ClimateServiceDiscovery.class.getName());
  
    private final String requiredServiceType;
    private final String requiredServiceName;
    private JmDNS jmdns;
    private volatile String discoveredHost;
    private volatile int discoveredPort;


    // Constructor

    public ClimateServiceDiscovery(String serviceType, String serviceName) {
        this.requiredServiceType = serviceType;
        this.requiredServiceName = serviceName;
    }

    // Discovery
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


    // getters 

    public String getDiscoveredHost() {
        return discoveredHost;
    }

    public int getDiscoveredPort() {
        return discoveredPort;
    }

    // Shutdown
    public void close() throws IOException {
        if (jmdns != null) {
            jmdns.close();
            logger.info("ClimateServiceDiscovery closed for [" + requiredServiceName + "]");
        }
    }
}
