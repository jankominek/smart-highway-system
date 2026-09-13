package com.jankominek.highwaycontracts.dto;

public record StolenVehicle(
        String plateNumber,
        boolean stolen,
        long updatedAt
) {
}
