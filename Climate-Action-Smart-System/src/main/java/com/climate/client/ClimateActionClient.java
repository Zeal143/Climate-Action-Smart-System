package com.climate.client;

import com.climate.discovery.ClimateServiceDiscovery;
import com.climate.grpc.*;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.Deadline;
import io.grpc.ForwardingClientCall;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;


public class ClimateActionClient {

    private static final Logger logger
            = Logger.getLogger(ClimateActionClient.class.getName());

    private static final String SERVICE_TYPE = "_grpc._tcp.local.";

    static final Metadata.Key<String> CLIENT_ID_KEY
            = Metadata.Key.of("client-id", Metadata.ASCII_STRING_MARSHALLER);

    // gRPC channels and stubs 
    private ManagedChannel airQualityChannel;
    private ManagedChannel floodRiskChannel;
    private ManagedChannel solarPanelChannel;

    private AirQualityServiceGrpc.AirQualityServiceBlockingStub airQualityStub;
    private AirQualityServiceGrpc.AirQualityServiceStub airQualityAsyncStub;

    private FloodRiskServiceGrpc.FloodRiskServiceStub floodRiskAsyncStub;

    private SolarPanelServiceGrpc.SolarPanelServiceBlockingStub solarPanelStub;
    private SolarPanelServiceGrpc.SolarPanelServiceStub solarPanelAsyncStub;

    
    // discover all three services via JmDNS

    public void init() throws IOException, InterruptedException {

        ClientInterceptor authInterceptor = new HeaderClientInterceptor("climate-gui-client");

        airQualityChannel = openChannel("AirQualityService", authInterceptor);
        floodRiskChannel = openChannel("FloodRiskService", authInterceptor);
        solarPanelChannel = openChannel("SolarPanelService", authInterceptor);

        airQualityStub = AirQualityServiceGrpc.newBlockingStub(airQualityChannel);
        airQualityAsyncStub = AirQualityServiceGrpc.newStub(airQualityChannel);

        floodRiskAsyncStub = FloodRiskServiceGrpc.newStub(floodRiskChannel);

        solarPanelStub = SolarPanelServiceGrpc.newBlockingStub(solarPanelChannel);
        solarPanelAsyncStub = SolarPanelServiceGrpc.newStub(solarPanelChannel);

        logger.info("ClimateActionClient: all channels connected.");
    }

    //  Client-side interceptor

    static class HeaderClientInterceptor implements ClientInterceptor {

        private final String clientId;

        HeaderClientInterceptor(String clientId) {
            this.clientId = clientId;
        }

        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                MethodDescriptor<ReqT, RespT> method,
                CallOptions callOptions,
                Channel next) {

            return new ForwardingClientCall.SimpleForwardingClientCall<>(
                    next.newCall(method, callOptions)) {

                @Override
                public void start(Listener<RespT> responseListener, Metadata headers) {
                    // Inject our client-id into the outgoing headers
                    headers.put(CLIENT_ID_KEY, clientId);
                    super.start(responseListener, headers);
                }
            };
        }
    }

    private ManagedChannel openChannel(String serviceName, ClientInterceptor interceptor)
            throws IOException, InterruptedException {

        ClimateServiceDiscovery discovery
                = new ClimateServiceDiscovery(SERVICE_TYPE, serviceName);
        String address = discovery.discoverService(15_000);
        discovery.close();

        String host;
        int port;

        if (address != null) {
            String[] parts = address.split(":");
            host = parts[0];
            port = Integer.parseInt(parts[1]);
            logger.info("Discovered " + serviceName + " at " + host + ":" + port + " (mDNS)");
        } else {
            // Fallback to default localhost ports
            host = "localhost";
            port = switch (serviceName) {
                case "AirQualityService" -> 50051;
                case "FloodRiskService"  -> 50052;
                case "SolarPanelService" -> 50053;
                default -> throw new IOException("Unknown service: " + serviceName);
            };
            logger.warning("Discovery timed out for " + serviceName
                    + " – falling back to " + host + ":" + port);
        }

        return ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .intercept(interceptor)
                .build();
    }

    // Air Quality Service methods

    public AirQualityResponse getCurrentAirQuality(String location) {
        AirQualityRequest request = AirQualityRequest.newBuilder()
                .setLocation(location)
                .build();

        logger.info("[Client] GetCurrentAirQuality: " + location);

        return airQualityStub
                .withDeadline(Deadline.after(5, TimeUnit.SECONDS))
                .getCurrentAirQuality(request);
    }

    public void streamAirQualityReadings(String location,
            AirQualityStreamListener listener) {

        AirQualityRequest request = AirQualityRequest.newBuilder()
                .setLocation(location)
                .build();

        logger.info("[Client] StreamAirQualityReadings: " + location);

        airQualityAsyncStub.streamAirQualityReadings(request, new StreamObserver<>() {
            @Override
            public void onNext(AirQualityReading reading) {
                listener.onReading(reading);
            }

            @Override
            public void onError(Throwable t) {
                logger.warning("[Stream] Air quality error: " + t.getMessage());
                listener.onError(t);
            }

            @Override
            public void onCompleted() {
                logger.info("[Stream] Air quality stream complete.");
                listener.onCompleted();
            }
        });
    }


     //Callback interface 

    public interface AirQualityStreamListener {

        void onReading(AirQualityReading reading);

        void onError(Throwable t);

        void onCompleted();
    }

    // Flood Risk Service methods

    public FloodRiskResponse reportRainfallReadings(List<RainfallRequest> readings)
            throws InterruptedException {

        logger.info("[Client] ReportRainfallReadings: sending " + readings.size() + " readings.");

        final FloodRiskResponse[] result = new FloodRiskResponse[1];
        final Throwable[] error = new Throwable[1];
        CountDownLatch latch = new CountDownLatch(1);

        StreamObserver<RainfallRequest> requestObserver
                = floodRiskAsyncStub.reportRainfallReadings(new StreamObserver<>() {
                    @Override
                    public void onNext(FloodRiskResponse response) {
                        result[0] = response;
                    }

                    @Override
                    public void onError(Throwable t) {
                        error[0] = t;
                        latch.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        latch.countDown();
                    }
                });

        for (RainfallRequest r : readings) {
            requestObserver.onNext(r);
        }
        requestObserver.onCompleted();

        latch.await(10, TimeUnit.SECONDS);
        if (error[0] != null) {
            throw new StatusRuntimeException(
                    io.grpc.Status.fromThrowable(error[0]));
        }
        return result[0];
    }

    // Bidirectional Streaming RPC 

    public StreamObserver<FloodUpdate> monitorFloodAlerts(
            List<FloodUpdate> updates, FloodAlertListener listener) throws InterruptedException {

        logger.info("[Client] MonitorFloodAlerts: streaming " + updates.size() + " updates.");

        CountDownLatch latch = new CountDownLatch(1);

        StreamObserver<FloodUpdate> requestObserver
                = floodRiskAsyncStub.monitorFloodAlerts(new StreamObserver<>() {
                    @Override
                    public void onNext(FloodAlert alert) {
                        listener.onAlert(alert);
                    }

                    @Override
                    public void onError(Throwable t) {
                        logger.warning("[BiDi] Flood alert error: " + t.getMessage());
                        listener.onError(t);
                        latch.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        logger.info("[BiDi] Flood monitoring session completed.");
                        listener.onCompleted();
                        latch.countDown();
                    }
                });

        // Send all updates then signal end of client stream
        for (FloodUpdate u : updates) {
            requestObserver.onNext(u);
        }
        requestObserver.onCompleted();

        // Wait for server to finish responding
        latch.await(10, TimeUnit.SECONDS);
        return requestObserver;
    }


     //Callback interface for bidirectional flood alert stream

    public interface FloodAlertListener {

        void onAlert(FloodAlert alert);

        void onError(Throwable t);

        void onCompleted();
    }

    // Solar Panel Service methods

    public PanelStatusResponse getPanelStatus(String panelId) {
        PanelRequest request = PanelRequest.newBuilder().setPanelId(panelId).build();
        logger.info("[Client] GetPanelStatus: " + panelId);
        return solarPanelStub
                .withDeadline(Deadline.after(5, TimeUnit.SECONDS))
                .getPanelStatus(request);
    }


     // Unary RPC – adjust the tilt angle of a solar panel.

    public ConfigResponse adjustPanel(String panelId, float angle) {
        ConfigRequest request = ConfigRequest.newBuilder()
                .setPanelId(panelId)
                .setNewAngle(angle)
                .build();
        logger.info("[Client] AdjustPanelConfiguration: " + panelId + " angle=" + angle);
        return solarPanelStub
                .withDeadline(Deadline.after(5, TimeUnit.SECONDS))
                .adjustPanelConfiguration(request);
    }


    // Server-side Streaming RPC – stream live energy readings.

    public void streamEnergyProduction(String panelId, EnergyStreamListener listener) {
        PanelRequest request = PanelRequest.newBuilder().setPanelId(panelId).build();
        logger.info("[Client] StreamEnergyProduction: " + panelId);

        solarPanelAsyncStub.streamEnergyProduction(request, new StreamObserver<>() {
            @Override
            public void onNext(EnergyReading r) {
                listener.onReading(r);
            }

            @Override
            public void onError(Throwable t) {
                listener.onError(t);
            }

            @Override
            public void onCompleted() {
                listener.onCompleted();
            }
        });
    }


    // Callback interface for server-streaming energy readings

    public interface EnergyStreamListener {

        void onReading(EnergyReading reading);

        void onError(Throwable t);

        void onCompleted();
    }


    public EnergyBatchSummary uploadEnergyBatch(List<EnergyBatchEntry> entries)
            throws InterruptedException {

        logger.info("[Client] UploadEnergyBatch: " + entries.size() + " entries.");

        final EnergyBatchSummary[] result = new EnergyBatchSummary[1];
        final Throwable[] error = new Throwable[1];
        CountDownLatch latch = new CountDownLatch(1);

        StreamObserver<EnergyBatchEntry> requestObserver
                = solarPanelAsyncStub.uploadEnergyBatch(new StreamObserver<>() {
                    @Override
                    public void onNext(EnergyBatchSummary s) {
                        result[0] = s;
                    }

                    @Override
                    public void onError(Throwable t) {
                        error[0] = t;
                        latch.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        latch.countDown();
                    }
                });

        for (EnergyBatchEntry e : entries) {
            requestObserver.onNext(e);
        }
        requestObserver.onCompleted();

        latch.await(10, TimeUnit.SECONDS);
        if (error[0] != null) {
            throw new StatusRuntimeException(
                    io.grpc.Status.fromThrowable(error[0]));
        }
        return result[0];
    }

    // Shutdown
    public void shutdown() throws InterruptedException {
        for (ManagedChannel ch : List.of(airQualityChannel, floodRiskChannel, solarPanelChannel)) {
            if (ch != null) {
                ch.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            }
        }
        logger.info("ClimateActionClient: all channels shut down.");
    }

    // main

    public static void main(String[] args) throws Exception {
        ClimateActionClient client = new ClimateActionClient();
        client.init();

        // 1. Air Quality – Unary
        System.out.println("\n=== Air Quality (Unary) ===");
        AirQualityResponse aqr = client.getCurrentAirQuality("DUBLIN");
        System.out.println("AQI=" + aqr.getAqi() + " Status=" + aqr.getStatus());

        // 2. Flood Risk – Client Streaming
        System.out.println("\n=== Flood Risk (Client Stream) ===");
        List<RainfallRequest> rainfall = List.of(
                RainfallRequest.newBuilder().setSensorId("S1").setRainfallMM(15.0f).setLocation("WICKLOW").build(),
                RainfallRequest.newBuilder().setSensorId("S2").setRainfallMM(45.0f).setLocation("WICKLOW").build(),
                RainfallRequest.newBuilder().setSensorId("S3").setRainfallMM(75.0f).setLocation("WICKLOW").build()
        );
        FloodRiskResponse frr = client.reportRainfallReadings(rainfall);
        System.out.println("Risk=" + frr.getRiskLevel() + " Avg=" + frr.getAverageRainfall());

        // 3. Solar Panel – Unary
        System.out.println("\n=== Solar Panel (Unary) ===");
        PanelStatusResponse psr = client.getPanelStatus("PANEL_DUBLIN_01");
        System.out.println("Status=" + psr.getStatus() + " Power=" + psr.getPowerOutput() + "W");

        client.shutdown();
    }
}
