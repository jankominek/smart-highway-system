package com.jankominek.highwayprocessor.configuration;

import com.jankominek.highwaycontracts.dto.AlertType;
import com.jankominek.highwaycontracts.dto.GantryScanEvent;
import com.jankominek.highwaycontracts.dto.HighwayAlert;
import com.jankominek.highwayprocessor.processor.SpeedingProcessor;
import com.jankominek.highwayprocessor.processor.SpeedingProcessorSupplier;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;

@Configuration
@EnableKafkaStreams
public class HighwayTopology {

    @Bean
    public KStream<String, HighwayAlert> speedingStream(
            StreamsBuilder builder,
            HighwayProperties properties
    ) {
        JacksonJsonSerde<GantryScanEvent> scanSerde =
                new JacksonJsonSerde<>(
                        GantryScanEvent.class
                );

        JacksonJsonSerde<HighwayAlert> alertSerde =
                new JacksonJsonSerde<>(
                        HighwayAlert.class
                );

        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(
                        SpeedingProcessor.LAST_SCAN_STORE
                        ),
                        Serdes.String(),
                        new JacksonJsonSerde<>(
                                GantryScanEvent.class
                        )
                )
        );

        KStream<String, GantryScanEvent> scans =
                builder.stream(
                        properties.inputTopic(),
                        Consumed.with(
                                Serdes.String(),
                                scanSerde
                        )
                );

        KStream<String, GantryScanEvent> validScans = scans.filter((key, event) ->
                event != null
                        && event.getPlateNumber() != null
                        && !event.getPlateNumber()
                        .isBlank()
                        && event.getGantryId() != null
                        && !event.getGantryId()
                        .isBlank()
        );

        validScans.to(
                properties.alertsTopic(),
                Produced.with(
                        Serdes.String(),
                        scanSerde
                )
        );

        KStream<String, GantryScanEvent> scansByPlate = validScans
                        .selectKey(
                                (ignoredKey, event) ->
                                        event.getPlateNumber()
                        )
                        .repartition(
                                Repartitioned.with(
                                        Serdes.String(),
                                        scanSerde
                                )
                        );


        KStream<String, HighwayAlert> stolenScans = scansByPlate
                .filter((plateNumber, event) -> plateNumber.contains("STOLEN"))
                .map((plateNumber, event) -> {
                    HighwayAlert stolenVahicleAlert = HighwayAlert.fromGantryScanEvent(
                            event,
                            "Stolen vehicle detected",
                            AlertType.STOLEN_VEHICLE
                    );
                    return new KeyValue<>(plateNumber, stolenVahicleAlert);
                });

        KStream<Windowed<String>, Long> trafficJamScanKTable = validScans
                .selectKey((ignoredKey, event) ->
                        event.getGantryId()
                )
                .groupByKey(
                        Grouped.with(
                                Serdes.String(),
                                scanSerde
                        )
                )
                .windowedBy(
                        TimeWindows.ofSizeAndGrace(
                                properties.trafficJam().window(),
                                properties.trafficJam().gracePeriod()
                        )
                ).count()
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream();

        trafficJamScanKTable
                .filter((windowedGantryId, count) ->
                        count >= properties.trafficJam().threshold()
                )
                .map((windowedGantryId, count) -> {
                    String gantryId = windowedGantryId.key();

                    HighwayAlert trafficJamAlert = HighwayAlert.builder()
                            .gantryId(gantryId)
                            .message("Traffic jam detected")
                            .vehicleCount(count)
                            .type(AlertType.TRAFFIC_JAM)
                            .build();
                    return new KeyValue<>(gantryId, trafficJamAlert);
                });

        KStream<String, HighwayAlert> speedingAlerts =
                scansByPlate.process(
                        new SpeedingProcessorSupplier(
                                properties.speeding()
                        ),
                        SpeedingProcessor.LAST_SCAN_STORE
                ).peek((key, alert) ->
                        System.out.println(
                                "Speeding alert: " + alert
                        )
                );

        stolenScans.to(
                properties.stolenVehiclesTopic(),
                Produced.with(
                        Serdes.String(),
                        alertSerde
                )
        );

        speedingAlerts.to(
                properties.speedingAlertsTopic(),
                Produced.with(
                        Serdes.String(),
                        alertSerde
                )
        );

        return speedingAlerts;
    }
}