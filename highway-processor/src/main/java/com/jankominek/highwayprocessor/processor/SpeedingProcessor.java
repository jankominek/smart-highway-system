package com.jankominek.highwayprocessor.processor;

import com.jankominek.highwaycontracts.dto.AlertType;
import com.jankominek.highwaycontracts.dto.GantryScanEvent;
import com.jankominek.highwaycontracts.dto.HighwayAlert;
import com.jankominek.highwayprocessor.configuration.HighwayProperties;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

public final class SpeedingProcessor implements Processor<
                String,
                GantryScanEvent,
                String,
                HighwayAlert> {

    public static final String LAST_SCAN_STORE =
            "last-scan-by-plate";

    private final HighwayProperties.Speeding properties;

    private KeyValueStore<
            String,
            GantryScanEvent
            > lastScans;

    private ProcessorContext<
            String,
            HighwayAlert
            > context;

    public SpeedingProcessor(
            HighwayProperties.Speeding properties
    ) {
        this.properties = properties;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(
            ProcessorContext<String, HighwayAlert> context
    ) {
        this.context = context;

        this.lastScans =
                (KeyValueStore<String, GantryScanEvent>)
                        context.getStateStore(
                                LAST_SCAN_STORE
                        );
    }

    @Override
    public void process(
            Record<String, GantryScanEvent> record
    ) {
        String plateNumber = record.key();
        GantryScanEvent current = record.value();

        GantryScanEvent previous =
                lastScans.get(plateNumber);

        lastScans.put(plateNumber, current);

        if (previous == null) {
            return;
        }

        long elapsedMillis =
                current.getTimestamp()
                        - previous.getTimestamp();

        if (elapsedMillis <= 0
                || elapsedMillis
                > properties.maxInterval().toMillis()) {
            return;
        }

        String route = "GANTRY" + previous.getGantryId() + current.getGantryId();

        Double distanceKm =
                properties.distancesKm().get(route);

        if (distanceKm == null) {
            return;
        }

        double elapsedHours =
                elapsedMillis / 3_600_000.0;

        double speedKmh =
                distanceKm / elapsedHours;

        if (speedKmh
                <= properties.speedLimitKmh()) {
            return;
        }

        HighwayAlert alert =
                new HighwayAlert(
                        "speeding:"
                                + plateNumber
                                + ":"
                                + current.getTimestamp(),
                        AlertType.SPEEDING,
                        plateNumber,
                        current.getGantryId(),
                        current.getTimestamp(),
                        speedKmh,
                        null,
                        "Przekroczono limit na trasie "
                                + route
                                + ": "
                                + speedKmh
                                + " km/h"
                );

        context.forward(
                new Record<>(
                        plateNumber,
                        alert,
                        record.timestamp()
                )
        );
    }

    @Override
    public void close() {
    }
}
