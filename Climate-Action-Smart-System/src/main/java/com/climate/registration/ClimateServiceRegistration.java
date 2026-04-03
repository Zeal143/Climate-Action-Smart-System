package com.climate.registration;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.logging.Logger;

import javax.jmdns.JmDNS;
import javax.jmdns.ServiceInfo;


public class ClimateServiceRegistration {

    private static final Logger logger =
            Logger.getLogger(ClimateServiceRegistration.class.getName());
    private static JmDNS jmdns;
    private static ClimateServiceRegistration instance;


    // Private constructor 

    private ClimateServiceRegistration() throws IOException {
        InetAddress localHost = InetAddress.getLocalHost();
        jmdns = JmDNS.create(localHost);
        logger.info("ClimateServiceRegistration: JmDNS created on " + localHost);
    }

    public static synchronized ClimateServiceRegistration getInstance() throws IOException {
        if (instance == null) {
            instance = new ClimateServiceRegistration();
        }
        return instance;
    }


    public void registerService(String type, String name, int port, String text)
            throws IOException {

        // Build the JmDNS ServiceInfo descriptor
        ServiceInfo serviceInfo = ServiceInfo.create(type, name, port, text);

        // Advertise on the local network
        jmdns.registerService(serviceInfo);

        logger.info("Registered: [" + name + "] type=" + type
                + " port=" + port + " text=" + text);
    }

    public void close() throws IOException {
        if (jmdns != null) {
            jmdns.unregisterAllServices();
            jmdns.close();
            logger.info("ClimateServiceRegistration: JmDNS closed.");
        }
    }
}
