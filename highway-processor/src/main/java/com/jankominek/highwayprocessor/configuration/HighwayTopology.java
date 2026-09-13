package com.jankominek.highwayprocessor.configuration;

import com.jankominek.highwaycontracts.dto.GantryScanEvent;
import com.jankominek.highwaycontracts.dto.HighwayAlert;
import com.jankominek.highwaycontracts.dto.StolenVehicle;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;

@Configuration
@EnableKafkaStreams
public class HighwayTopology {
    JacksonJsonSerde<GantryScanEvent> gantryScanSerde = new JacksonJsonSerde<>(GantryScanEvent.class);
    JacksonJsonSerde<HighwayAlert> highwayAlertSerde = new JacksonJsonSerde<>(HighwayAlert.class);
    JacksonJsonSerde<StolenVehicle> stolenVehiclesSerde = new JacksonJsonSerde<>(StolenVehicle.class);

    @Bean
    public KStream<String, GantryScanEvent> highwayStream(StreamsBuilder streamsBuilder, HighwayProperties highwayProperties) {
        KStream<String, GantryScanEvent> gantryScanStream = streamsBuilder.stream(
                highwayProperties.inputTopic(),
                Consumed.with(
                        Serdes.String(),
                        gantryScanSerde
                )
        );

        KStream<String, GantryScanEvent> validGantryScans = gantryScanStream.filter(
                (key, event) -> event != null
                && event.getPlateNumber() != null
                && event.getGantryId() != null
        );

        validGantryScans.to(
                "highway-processed",
                Produced.with(
                        Serdes.String(),
                        gantryScanSerde)
        );

        return validGantryScans;
    }
}
