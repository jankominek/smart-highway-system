package com.jankominek.highwaycontracts.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class GantryScanEvent {
    private String gantryId;
    private String plateNumber;
    private long timestamp;
}
