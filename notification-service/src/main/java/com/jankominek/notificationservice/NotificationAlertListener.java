package com.jankominek.notificationservice;

import com.jankominek.highwaycontracts.dto.AlertType;
import com.jankominek.highwaycontracts.dto.HighwayAlert;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

@Component
@Slf4j
public class NotificationAlertListener {

    private static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern(
                    "yyyy-MM-dd HH:mm:ss z",
                    Locale.ROOT
            ).withZone(ZoneId.systemDefault());

    @KafkaListener(
            topics = {
                    "${app.notification.topics.speeding-alerts}",
                    "${app.notification.topics.stolen-vehicles}",
                    "${app.notification.topics.traffic-jam-alerts}",
                    "${app.notification.topics.highway-alerts}"
            },
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void receiveAlert(HighwayAlert alert) {
        log.info("{}", formatAlert(alert));
    }

    private String formatAlert(HighwayAlert alert) {
        String type = alert.type() == null
                ? "UNKNOWN"
                : alert.type().name();

        String icon = switch (alert.type()) {
            case NORMAL_TRAFFIC -> "🟢";
            case SPEEDING -> "‼️";
            case STOLEN_VEHICLE -> "🚓";
            case TRAFFIC_JAM -> "🚧";
            case null -> "❔";
        };

        String title = switch (alert.type()) {
            case NORMAL_TRAFFIC -> "NORMALNY RUCH";
            case SPEEDING -> "PRZEKROCZENIE PRĘDKOŚCI";
            case STOLEN_VEHICLE -> "SKRADZIONY POJAZD";
            case TRAFFIC_JAM -> "KOREK";
            case null -> "NIEZNANY ALERT";
        };

        String details = switch (alert.type()) {
            case NORMAL_TRAFFIC -> "Pojazd przejechał przez bramkę";
            case SPEEDING -> "Prędkość: "
                    + formatSpeed(alert.measuredSpeedKmh());
            case STOLEN_VEHICLE -> "Pojazd oznaczony jako skradziony";
            case TRAFFIC_JAM -> "Liczba pojazdów: "
                    + valueOrDash(alert.vehicleCount());
            case null -> valueOrDash(alert.message());
        };

        return """
        %s %s
        ----------------------------------------
        %s
        Tablica: %s
        Brama: %s
        Czas: %s
        ----------------------------------------
        """.formatted(
                icon,
                title,
                details,
                valueOrDash(alert.plateNumber()),
                valueOrDash(alert.gantryId()),
                DATE_TIME_FORMATTER.format(
                        Instant.ofEpochMilli(alert.detectedAt())
                )
        );
    }

    private String valueOrDash(String value) {
        return value == null || value.isBlank() ? "-" : value;
    }

    private String valueOrDash(Long value) {
        return value == null ? "-" : value.toString();
    }

    private String formatSpeed(Double speedKmh) {
        return speedKmh == null
                ? "-"
                : String.format(Locale.ROOT, "%.2f km/h", speedKmh);
    }
}
