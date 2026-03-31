package com.climate.registration;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.logging.Logger;

import javax.jmdns.JmDNS;
import javax.jmdns.ServiceInfo;

/**
 * ClimateServiceRegistration – Singleton JmDNS registration manager.
 *
 * All three climate services (Air Quality, Flood Risk, Solar Panel) call
 * getInstance() to obtain the single shared JmDNS instance and then call
 * registerService() to advertise themselves on the local network.
 *
 * Pattern mirrors ExampleServiceRegistration from the course labs.
 */
public class ClimateServiceRegistration {

    private static final Logger logger =
            Logger.getLogger(ClimateServiceRegistration.class.getName());

    /** Shared JmDNS multicast-DNS instance */
    private static JmDNS jmdns;

    /** The single instance of this class (Singleton) */
    private static ClimateServiceRegistration instance;

    // -----------------------------------------------------------------------
    // Private constructor – creates the JmDNS instance bound to localhost
    // -----------------------------------------------------------------------
    private ClimateServiceRegistration() throws IOException {
        InetAddress localHost = InetAddress.getLocalHost();
        jmdns = JmDNS.create(localHost);
        logger.info("ClimateServiceRegistration: JmDNS created on " + localHost);
    }

    // -----------------------------------------------------------------------
    // Public factory – returns (or creates) the singleton instance
    // -----------------------------------------------------------------------

    /**
     * Returns the singleton registration manager.
     * Thread-safe via simple synchronisation; services register during startup.
     *
     * @return the single ClimateServiceRegistration instance
     * @throws IOException if JmDNS cannot bind to the local address
     */
    public static synchronized ClimateServiceRegistration getInstance() throws IOException {
        if (instance == null) {
            instance = new ClimateServiceRegistration();
        }
        return instance;
    }

    // -----------------------------------------------------------------------
    // Service registration
    // -----------------------------------------------------------------------

    /**
     * Registers a service with JmDNS so that clients can discover it via mDNS.
     *
     * @param type  Fully-qualified service type, e.g. "_grpc._tcp.local."
     * @param name  Human-readable service name, e.g. "AirQualityService"
     * @param port  Port the gRPC service listens on
     * @param text  TXT record, typically "path=<service-name>" for routing hints
     * @throws IOException if registration fails
     */
    public void registerService(String type, String name, int port, String text)
            throws IOException {

        // Build the JmDNS ServiceInfo descriptor
        ServiceInfo serviceInfo = ServiceInfo.create(type, name, port, text);

        // Advertise on the local network
        jmdns.registerService(serviceInfo);

        logger.info("Registered: [" + name + "] type=" + type
                + " port=" + port + " text=" + text);
    }

    // -----------------------------------------------------------------------
    // Shutdown
    // -----------------------------------------------------------------------

    /**
     * Unregisters all services and closes the JmDNS socket.
     * Call this when the server shuts down to release multicast membership.
     */
    public void close() throws IOException {
        if (jmdns != null) {
            jmdns.unregisterAllServices();
            jmdns.close();
            logger.info("ClimateServiceRegistration: JmDNS closed.");
        }
    }
}
