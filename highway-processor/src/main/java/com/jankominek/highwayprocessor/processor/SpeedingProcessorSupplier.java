package com.jankominek.highwayprocessor.processor;

import com.jankominek.highwaycontracts.dto.GantryScanEvent;
import com.jankominek.highwaycontracts.dto.HighwayAlert;
import com.jankominek.highwayprocessor.configuration.HighwayProperties;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

public final class SpeedingProcessorSupplier
        implements ProcessorSupplier<
                String,
                GantryScanEvent,
                String,
                HighwayAlert
        > {

    private final HighwayProperties.Speeding properties;

    public SpeedingProcessorSupplier(
            HighwayProperties.Speeding properties
    ) {
        this.properties = properties;
    }

    @Override
    public Processor<
            String,
            GantryScanEvent,
            String,
            HighwayAlert
    > get() {
        return new SpeedingProcessor(properties);
    }
}
