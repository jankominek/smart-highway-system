package com.jankominek.highwaycontracts.dto;

import lombok.Builder;

@Builder
public record HighwayAlert(
        String id,
        AlertType type,
        String plateNumber,
        String gantryId,
        long detectedAt,
        Double measuredSpeedKmh,
        Long vehicleCount,
        String message
) {

    public static HighwayAlert fromGantryScanEvent(GantryScanEvent gantryScanEvent, String message, AlertType type) {
        return  HighwayAlert.builder()
                .id(gantryScanEvent.getPlateNumber() + "-" + gantryScanEvent.getGantryId() + "-" + gantryScanEvent.getTimestamp())
                .type(type)
                .plateNumber(gantryScanEvent.getPlateNumber())
                .gantryId(gantryScanEvent.getGantryId())
                .detectedAt(System.currentTimeMillis())
                .measuredSpeedKmh(null)
                .vehicleCount(null)
                .message(message)
                .build();
    }
}