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
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * SolarPanelServer – gRPC server for the Solar Panel Management service.
 *
 * Implements three RPC styles (plus a second Unary):
 *   1. Unary RPC             – GetPanelStatus
 *   2. Unary RPC             – AdjustPanelConfiguration
 *   3. Server-side Streaming – StreamEnergyProduction
 *   4. Client-side Streaming – UploadEnergyBatch
 *
 * Registers itself with JmDNS on startup.
 *
 * UN SDG 13: Climate Action
 */
public class SolarPanelServer {

    private static final Logger logger = Logger.getLogger(SolarPanelServer.class.getName());

    static final int PORT = 50053;
    static final String SERVICE_TYPE = "_grpc._tcp.local.";
    static final String SERVICE_NAME = "SolarPanelService";

    public static void main(String[] args) throws IOException, InterruptedException {

        Server server = ServerBuilder.forPort(PORT)
                .addService(new SolarPanelServiceImpl())
                .intercept(new MetadataLoggingInterceptor())
                .build()
                .start();

        logger.info("SolarPanelServer started on port " + PORT);

        ClimateServiceRegistration reg = ClimateServiceRegistration.getInstance();
        reg.registerService(SERVICE_TYPE, SERVICE_NAME, PORT,
                "path=SolarPanelService description=SolarPanelManagement");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down SolarPanelServer...");
            server.shutdown();
            try { reg.close(); } catch (IOException ignored) {}
        }));

        server.awaitTermination();
    }

    // =========================================================================
    // Service Implementation
    // =========================================================================

    static class SolarPanelServiceImpl extends SolarPanelServiceGrpc.SolarPanelServiceImplBase {

        private static final Random  random = new Random();
        private static final DateTimeFormatter FMT =
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

        // Simulated panel registry (panelId → current tilt angle)
        private static final java.util.Map<String, Float> panelAngles =
                new java.util.concurrent.ConcurrentHashMap<>();

        static {
            panelAngles.put("PANEL_DUBLIN_01", 35.0f);
            panelAngles.put("PANEL_DUBLIN_02", 40.0f);
            panelAngles.put("PANEL_CORK_01",   38.0f);
        }

        // ── Unary RPC: GetPanelStatus ─────────────────────────────────────────
        @Override
        public void getPanelStatus(PanelRequest request,
                                   StreamObserver<PanelStatusResponse> responseObserver) {

            String panelId = request.getPanelId().trim();
            logger.info("[Unary] GetPanelStatus for: " + panelId);

            // ── Remote Error Handling ─────────────────────────────────────────
            if (panelId.isEmpty()) {
                responseObserver.onError(Status.INVALID_ARGUMENT
                        .withDescription("Panel ID must not be empty.")
                        .asRuntimeException());
                return;
            }
            if (!panelAngles.containsKey(panelId)) {
                responseObserver.onError(Status.NOT_FOUND
                        .withDescription("Panel not found: " + panelId
                                + ". Known panels: " + panelAngles.keySet())
                        .asRuntimeException());
                return;
            }

            float power       = 150 + random.nextFloat() * 350;
            float efficiency  = 60 + random.nextFloat() * 35;
            float temperature = 25 + random.nextFloat() * 30;
            String status     = efficiency > 80 ? "ACTIVE"
                              : efficiency > 50 ? "DEGRADED" : "OFFLINE";

            PanelStatusResponse response = PanelStatusResponse.newBuilder()
                    .setPanelId(panelId)
                    .setPowerOutput(power)
                    .setEfficiency(efficiency)
                    .setStatus(status)
                    .setTemperature(temperature)
                    .setLastUpdated(LocalDateTime.now().format(FMT))
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
            logger.info("[Unary] Sent status=" + status
                    + " power=" + String.format("%.1f", power) + "W for " + panelId);
        }

        // ── Unary RPC: AdjustPanelConfiguration ──────────────────────────────
        @Override
        public void adjustPanelConfiguration(ConfigRequest request,
                                             StreamObserver<ConfigResponse> responseObserver) {

            String panelId = request.getPanelId().trim();
            float  angle   = request.getNewAngle();
            logger.info("[Unary] AdjustPanelConfiguration: panelId=" + panelId
                    + " angle=" + angle);

            // ── Remote Error Handling ─────────────────────────────────────────
            if (panelId.isEmpty()) {
                responseObserver.onError(Status.INVALID_ARGUMENT
                        .withDescription("Panel ID must not be empty.")
                        .asRuntimeException());
                return;
            }
            if (angle < 0 || angle > 90) {
                responseObserver.onError(Status.INVALID_ARGUMENT
                        .withDescription("Angle must be between 0 and 90 degrees. Got: " + angle)
                        .asRuntimeException());
                return;
            }
            if (!panelAngles.containsKey(panelId)) {
                responseObserver.onError(Status.NOT_FOUND
                        .withDescription("Panel not found: " + panelId)
                        .asRuntimeException());
                return;
            }

            // Apply the new angle
            panelAngles.put(panelId, angle);

            ConfigResponse response = ConfigResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("Panel " + panelId + " tilt angle adjusted to "
                            + String.format("%.1f", angle) + "°")
                    .setAppliedAngle(angle)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
            logger.info("[Unary] Angle updated for " + panelId + " → " + angle + "°");
        }

        // ── Server-side Streaming RPC: StreamEnergyProduction ─────────────────
        @Override
        public void streamEnergyProduction(PanelRequest request,
                                           StreamObserver<EnergyReading> responseObserver) {

            String panelId = request.getPanelId().trim();
            logger.info("[ServerStream] StreamEnergyProduction for: " + panelId);

            if (panelId.isEmpty()) {
                responseObserver.onError(Status.INVALID_ARGUMENT
                        .withDescription("Panel ID must not be empty.")
                        .asRuntimeException());
                return;
            }

            try {
                for (int i = 1; i <= 10; i++) {

                    // ── Cancellation support ──────────────────────────────────
                    if (Context.current().isCancelled()) {
                        logger.info("[ServerStream] Cancelled by client after " + i + " readings.");
                        responseObserver.onError(Status.CANCELLED
                                .withDescription("Energy stream cancelled by client.")
                                .asRuntimeException());
                        return;
                    }

                    float energy    = 0.5f + random.nextFloat() * 4.5f;
                    float sunlight  = 200 + random.nextFloat() * 800;

                    EnergyReading reading = EnergyReading.newBuilder()
                            .setEnergyKwh(energy)
                            .setSunlightLevel(sunlight)
                            .setTimestamp(LocalDateTime.now().format(FMT))
                            .setPanelId(panelId)
                            .build();

                    responseObserver.onNext(reading);
                    logger.info("[ServerStream] Reading " + i
                            + " energy=" + String.format("%.2f", energy) + " kWh"
                            + " sunlight=" + String.format("%.0f", sunlight) + " W/m²");

                    TimeUnit.SECONDS.sleep(1);
                }
                responseObserver.onCompleted();
                logger.info("[ServerStream] Energy stream completed for " + panelId);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                responseObserver.onError(Status.INTERNAL
                        .withDescription("Stream interrupted: " + e.getMessage())
                        .asRuntimeException());
            }
        }

        // ── Client-side Streaming RPC: UploadEnergyBatch ─────────────────────
        @Override
        public StreamObserver<EnergyBatchEntry> uploadEnergyBatch(
                StreamObserver<EnergyBatchSummary> responseObserver) {

            logger.info("[ClientStream] UploadEnergyBatch started.");

            return new StreamObserver<>() {

                private final List<Float> energyValues = new ArrayList<>();
                private int count = 0;

                @Override
                public void onNext(EnergyBatchEntry entry) {
                    if (entry.getEnergyKwh() < 0) {
                        responseObserver.onError(Status.INVALID_ARGUMENT
                                .withDescription("Energy value cannot be negative. "
                                        + "Panel: " + entry.getPanelId())
                                .asRuntimeException());
                        return;
                    }
                    energyValues.add(entry.getEnergyKwh());
                    count++;
                    logger.info("[ClientStream] Received batch entry from panelId="
                            + entry.getPanelId()
                            + " energy=" + entry.getEnergyKwh() + " kWh");
                }

                @Override
                public void onError(Throwable t) {
                    logger.warning("[ClientStream] Batch upload error: " + t.getMessage());
                }

                @Override
                public void onCompleted() {
                    if (count == 0) {
                        responseObserver.onError(Status.INVALID_ARGUMENT
                                .withDescription("No batch entries were uploaded.")
                                .asRuntimeException());
                        return;
                    }

                    float total = 0, peak = 0;
                    for (float v : energyValues) {
                        total += v;
                        peak   = Math.max(peak, v);
                    }
                    float avg = total / count;

                    EnergyBatchSummary summary = EnergyBatchSummary.newBuilder()
                            .setRecordsReceived(count)
                            .setTotalEnergyKwh(total)
                            .setAverageEnergyKwh(avg)
                            .setPeakEnergyKwh(peak)
                            .setMessage("Batch upload complete. Received " + count
                                    + " records. Total: " + String.format("%.2f", total)
                                    + " kWh. Average: " + String.format("%.2f", avg) + " kWh.")
                            .build();

                    responseObserver.onNext(summary);
                    responseObserver.onCompleted();
                    logger.info("[ClientStream] Batch complete. records=" + count
                            + " total=" + String.format("%.2f", total) + " kWh");
                }
            };
        }
    }

    // =========================================================================
    // Server Interceptor – Metadata logging
    // =========================================================================

    static class MetadataLoggingInterceptor implements ServerInterceptor {

        static final Metadata.Key<String> CLIENT_ID_KEY =
                Metadata.Key.of("client-id", Metadata.ASCII_STRING_MARSHALLER);

        @Override
        public <ReqT, RespT> Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> call,
                Metadata headers,
                ServerCallHandler<ReqT, RespT> next) {

            String clientId = headers.get(CLIENT_ID_KEY);
            logger.info("[Interceptor] Incoming call from client-id: "
                    + (clientId != null ? clientId : "unknown")
                    + " method=" + call.getMethodDescriptor().getFullMethodName());
            return next.startCall(call, headers);
        }
    }
}
