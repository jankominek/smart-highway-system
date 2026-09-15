package com.jankominek.highwayprocessor.configuration;


import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.Map;

@ConfigurationProperties(prefix = "app.highway")
public record HighwayProperties (
    String inputTopic,
    String speedingAlertsTopic,
    String alertsTopic,
    String stolenVehiclesTopic,
    String trafficJamAlertsTopic,
    Speeding speeding,
    TrafficJam trafficJam
) {
    public record Speeding (
            double speedLimitKmh,
            Duration maxInterval,
            Map<String, Double> distancesKm
    ){}

    public record TrafficJam(
            Duration window,
            Duration gracePeriod,
            long threshold
    ) { }
}

