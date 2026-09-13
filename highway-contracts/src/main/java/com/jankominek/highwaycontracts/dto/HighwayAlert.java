package com.jankominek.highwaycontracts.dto;

public record HighwayAlert(
        String id,
        AlertType type,
        String plateNumber,
        String gantryId,
        long detectedAt,
        Double measuredSpeedKmh,
        Long vehicleCount,
        String message
) { }