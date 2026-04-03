package com.climate.server;

import com.climate.grpc.*;
import com.climate.registration.ClimateServiceRegistration;

import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;


public class AirQualityServer {

    private static final Logger logger = Logger.getLogger(AirQualityServer.class.getName());

    static final int PORT = 50051;
    static final String SERVICE_TYPE = "_grpc._tcp.local.";
    static final String SERVICE_NAME = "AirQualityService";

    public static void main(String[] args) throws IOException, InterruptedException {

        // Build and start the gRPC server 
        Server server = ServerBuilder.forPort(PORT)
                .addService(new AirQualityServiceImpl())
                .intercept(new AuthInterceptor())          
                .build()
                .start();

        logger.info("AirQualityServer started on port " + PORT);

        // Register with JmDNS
        ClimateServiceRegistration reg = ClimateServiceRegistration.getInstance();
        reg.registerService(SERVICE_TYPE, SERVICE_NAME, PORT,
                "path=AirQualityService description=AirQualityMonitoring");

        // Shutdown hook 
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down AirQualityServer...");
            server.shutdown();
            try { reg.close(); } catch (IOException ignored) {}
        }));

        server.awaitTermination();
    }

    // Service Implementation

    static class AirQualityServiceImpl extends AirQualityServiceGrpc.AirQualityServiceImplBase {

        private static final Random random = new Random();
        private static final DateTimeFormatter FMT =
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

        // locations for validation
        private static final java.util.Set<String> KNOWN_LOCATIONS =
                java.util.Set.of("DUBLIN", "LONDON", "BEIJING", "PARIS", "BERLIN");

        // Unary RPC 
        @Override
        public void getCurrentAirQuality(AirQualityRequest request,
                                         StreamObserver<AirQualityResponse> responseObserver) {

            String location = request.getLocation().toUpperCase().trim();
            logger.info("[Unary] GetCurrentAirQuality for: " + location);

            // Remote Error Handling 
            if (location.isEmpty()) {
                responseObserver.onError(Status.INVALID_ARGUMENT
                        .withDescription("Location must not be empty.")
                        .asRuntimeException());
                return;
            }
            if (!KNOWN_LOCATIONS.contains(location)) {
                responseObserver.onError(Status.NOT_FOUND
                        .withDescription("Unknown location: '" + location
                                + "'. Known: " + KNOWN_LOCATIONS)
                        .asRuntimeException());
                return;
            }

            float aqi      = 20 + random.nextFloat() * 180;
            float co2Level = 400 + random.nextFloat() * 200;
            String status  = classifyAqi(aqi);

            AirQualityResponse response = AirQualityResponse.newBuilder()
                    .setAqi(aqi)
                    .setCo2Level(co2Level)
                    .setStatus(status)
                    .setLocation(location)
                    .setTimestamp(LocalDateTime.now().format(FMT))
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
            logger.info("[Unary] Sent AQI=" + String.format("%.1f", aqi)
                    + " status=" + status + " for " + location);
        }

        // Server-side Streaming RPC 
        @Override
        public void streamAirQualityReadings(AirQualityRequest request,
                                             StreamObserver<AirQualityReading> responseObserver) {

            String location = request.getLocation().toUpperCase().trim();
            logger.info("[ServerStream] StreamAirQualityReadings for: " + location);

            // Remote Error Handling 
            if (location.isEmpty()) {
                responseObserver.onError(Status.INVALID_ARGUMENT
                        .withDescription("Location must not be empty.")
                        .asRuntimeException());
                return;
            }

            // Stream 10 readings at 1-second intervals
            try {
                for (int i = 1; i <= 10; i++) {

                    // client cancellation (Deadline / cancel support)
                    if (Context.current().isCancelled()) {
                        logger.info("[ServerStream] Client cancelled. Stopping stream.");
                        responseObserver.onError(Status.CANCELLED
                                .withDescription("Stream cancelled by client.")
                                .asRuntimeException());
                        return;
                    }

                    float aqi      = 20 + random.nextFloat() * 180;
                    float co2Level = 400 + random.nextFloat() * 200;

                    AirQualityReading reading = AirQualityReading.newBuilder()
                            .setAqi(aqi)
                            .setCo2Level(co2Level)
                            .setTimestamp(LocalDateTime.now().format(FMT))
                            .setLocation(location)
                            .build();

                    responseObserver.onNext(reading);
                    logger.info("[ServerStream] Sent reading " + i
                            + " AQI=" + String.format("%.1f", aqi));

                    TimeUnit.SECONDS.sleep(1);
                }
                responseObserver.onCompleted();
                logger.info("[ServerStream] Completed stream for " + location);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                responseObserver.onError(Status.INTERNAL
                        .withDescription("Streaming interrupted: " + e.getMessage())
                        .asRuntimeException());
            }
        }

        private String classifyAqi(float aqi) {
            if (aqi <= 50)  return "GOOD";
            if (aqi <= 100) return "MODERATE";
            if (aqi <= 200) return "UNHEALTHY";
            return "HAZARDOUS";
        }
    }


    // Server Interceptor


    static class AuthInterceptor implements ServerInterceptor {

        static final Metadata.Key<String> CLIENT_ID_KEY =
                Metadata.Key.of("client-id", Metadata.ASCII_STRING_MARSHALLER);

        @Override
        public <ReqT, RespT> Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> call,
                Metadata headers,
                ServerCallHandler<ReqT, RespT> next) {

            String clientId = headers.get(CLIENT_ID_KEY);
            if (clientId == null || clientId.isBlank()) {
                logger.warning("[Auth] Request rejected – missing 'client-id' header.");
                call.close(Status.UNAUTHENTICATED
                        .withDescription("Missing 'client-id' metadata header."), new Metadata());
                return new ServerCall.Listener<>() {};
            }
            logger.info("[Auth] Accepted request from client-id: " + clientId);
            return next.startCall(call, headers);
        }
    }
}
