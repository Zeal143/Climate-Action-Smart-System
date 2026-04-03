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
import java.util.logging.Logger;


public class FloodRiskServer {

    private static final Logger logger = Logger.getLogger(FloodRiskServer.class.getName());

    static final int PORT = 50052;
    static final String SERVICE_TYPE = "_grpc._tcp.local.";
    static final String SERVICE_NAME = "FloodRiskService";

    public static void main(String[] args) throws IOException, InterruptedException {

        Server server = ServerBuilder.forPort(PORT)
                .addService(new FloodRiskServiceImpl())
                .intercept(new DeadlineInterceptor())    // Deadline / metadata demo
                .build()
                .start();

        logger.info("FloodRiskServer started on port " + PORT);

        ClimateServiceRegistration reg = ClimateServiceRegistration.getInstance();
        reg.registerService(SERVICE_TYPE, SERVICE_NAME, PORT,
                "path=FloodRiskService description=FloodRiskMonitoring");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down FloodRiskServer...");
            server.shutdown();
            try { reg.close(); } catch (IOException ignored) {}
        }));

        server.awaitTermination();
    }

    // Service Implementation

    static class FloodRiskServiceImpl extends FloodRiskServiceGrpc.FloodRiskServiceImplBase {

        private static final DateTimeFormatter FMT =
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

        // Client-side Streaming RPC 

        @Override
        public StreamObserver<RainfallRequest> reportRainfallReadings(
                StreamObserver<FloodRiskResponse> responseObserver) {

            logger.info("[ClientStream] ReportRainfallReadings started.");

            return new StreamObserver<>() {

                private final List<Float> readings = new ArrayList<>();
                private float maxRainfall = 0;
                private int count = 0;

                @Override
                public void onNext(RainfallRequest req) {
                    //Remote Error Handling
                    if (req.getRainfallMM() < 0) {
                        responseObserver.onError(Status.INVALID_ARGUMENT
                                .withDescription("Rainfall reading cannot be negative. "
                                        + "Sensor: " + req.getSensorId())
                                .asRuntimeException());
                        return;
                    }
                    float mm = req.getRainfallMM();
                    readings.add(mm);
                    maxRainfall = Math.max(maxRainfall, mm);
                    count++;
                    logger.info("[ClientStream] Received reading from sensor="
                            + req.getSensorId() + " rainfall=" + mm + " mm/hr");
                }

                @Override
                public void onError(Throwable t) {
                    logger.warning("[ClientStream] Client error: " + t.getMessage());
                }

                @Override
                public void onCompleted() {
                    if (count == 0) {
                        responseObserver.onError(Status.INVALID_ARGUMENT
                                .withDescription("No rainfall readings were sent.")
                                .asRuntimeException());
                        return;
                    }

                    double avg = readings.stream()
                            .mapToDouble(Float::doubleValue).average().orElse(0);
                    String riskLevel = classifyFloodRisk((float) avg, maxRainfall);

                    FloodRiskResponse response = FloodRiskResponse.newBuilder()
                            .setAverageRainfall((float) avg)
                            .setMaxRainfall(maxRainfall)
                            .setRiskLevel(riskLevel)
                            .setReadingsCount(count)
                            .setMessage("Processed " + count + " readings. Risk: " + riskLevel
                                    + ". Avg rainfall: " + String.format("%.2f", avg) + " mm/hr.")
                            .build();

                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                    logger.info("[ClientStream] Completed. Risk=" + riskLevel
                            + " readings=" + count);
                }
            };
        }

        //Bidirectional Streaming RPC

        @Override
        public StreamObserver<FloodUpdate> monitorFloodAlerts(
                StreamObserver<FloodAlert> responseObserver) {

            logger.info("[BiDiStream] MonitorFloodAlerts started.");

            return new StreamObserver<>() {

                @Override
                public void onNext(FloodUpdate update) {
                    logger.info("[BiDiStream] Received update from sensor="
                            + update.getSensorId()
                            + " waterLevel=" + update.getWaterLevel() + "m");

                    // Remote Error Handling 
                    if (update.getWaterLevel() < 0) {
                        responseObserver.onError(Status.INVALID_ARGUMENT
                                .withDescription("Water level cannot be negative. Sensor: "
                                        + update.getSensorId())
                                .asRuntimeException());
                        return;
                    }

                    // Check for client cancellation
                    if (Context.current().isCancelled()) {
                        logger.info("[BiDiStream] Context cancelled – stopping.");
                        responseObserver.onError(Status.CANCELLED
                                .withDescription("Monitoring cancelled by client.")
                                .asRuntimeException());
                        return;
                    }

                    String risk    = classifyWaterLevel(update.getWaterLevel());
                    String advice  = buildAdvice(risk, update.getLocation());

                    FloodAlert alert = FloodAlert.newBuilder()
                            .setSensorId(update.getSensorId())
                            .setRiskLevel(risk)
                            .setAdvisoryMessage(advice)
                            .setTimestamp(LocalDateTime.now().format(FMT))
                            .build();

                    responseObserver.onNext(alert);
                    logger.info("[BiDiStream] Sent alert risk=" + risk
                            + " for sensor=" + update.getSensorId());
                }

                @Override
                public void onError(Throwable t) {
                    logger.warning("[BiDiStream] Stream error: " + t.getMessage());
                }

                @Override
                public void onCompleted() {
                    responseObserver.onCompleted();
                    logger.info("[BiDiStream] Monitoring session completed.");
                }
            };
        }

        // Helpers

        private String classifyFloodRisk(float avgMM, float maxMM) {
            if (maxMM > 80 || avgMM > 50) return "CRITICAL";
            if (maxMM > 50 || avgMM > 30) return "HIGH";
            if (maxMM > 20 || avgMM > 10) return "MEDIUM";
            return "LOW";
        }

        private String classifyWaterLevel(float level) {
            if (level > 5.0f) return "CRITICAL";
            if (level > 3.0f) return "HIGH";
            if (level > 1.5f) return "MEDIUM";
            return "LOW";
        }

        private String buildAdvice(String risk, String location) {
            return switch (risk) {
                case "CRITICAL" -> "EVACUATE " + location + " immediately. Emergency services alerted.";
                case "HIGH"     -> "Flood barriers activated in " + location + ". Avoid flood zones.";
                case "MEDIUM"   -> "Monitor water levels in " + location + ". Stay alert.";
                default         -> "Conditions normal in " + location + ". No action required.";
            };
        }
    }

    // Server Interceptor 

    static class DeadlineInterceptor implements ServerInterceptor {

        static final Metadata.Key<String> DEADLINE_KEY =
                Metadata.Key.of("request-deadline", Metadata.ASCII_STRING_MARSHALLER);

        @Override
        public <ReqT, RespT> Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> call,
                Metadata headers,
                ServerCallHandler<ReqT, RespT> next) {

            String deadline = headers.get(DEADLINE_KEY);
            if (deadline != null) {
                logger.info("[Deadline] Client requested deadline: " + deadline);
            }
            return next.startCall(call, headers);
        }
    }
}
