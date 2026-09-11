package com.jankominek.camerasimulator.services;

import com.jankominek.camerasimulator.GantryScanEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Random;

@Service
@RequiredArgsConstructor
public class CameraSimulatorService {

    @Value("${app.kafka.topic.name}")
    private String topic;
    private final KafkaTemplate<String, GantryScanEvent> kafkaTemplate;
    private int counter = 0;
    private final Random random = new Random();
    private final List<String> gantryIds = List.of("GANTRY-A", "GANTRY-B", "GANTRY-C");

    private void sendGantryScanEvent(GantryScanEvent gantryScanEvent) {
        kafkaTemplate.send(topic, gantryScanEvent.getPlateNumber(), gantryScanEvent);
    }

    @Scheduled(fixedRate = 5000) // Wykonuje się co 5 sekundy
    public void generateTraffic() {
        counter++;

        // Co 5 cykli (co 10 sekund) wyzwalamy konkretny scenariusz testowy
        if (counter % 5 == 0) {
            triggerSpeedingScenario();
        } else if (counter % 7 == 0) {
            triggerStolenCarScenario();
        } else if (counter % 12 == 0) {
            triggerTrafficJamScenario();
        } else {
            generateNormalRandomTraffic();
        }
    }

    // 1. Ruch normalny
    private void generateNormalRandomTraffic() {
        String plate = "WA-" + (10000 + random.nextInt(90000));
        String gantry = gantryIds.get(random.nextInt(gantryIds.size()));
        long now = Instant.now().toEpochMilli();
        GantryScanEvent gantryScanEvent = GantryScanEvent.builder()
                        .gantryId(gantry)
                                .plateNumber(plate)
                                        .timestamp(now)
                                                .build();

        sendGantryScanEvent(gantryScanEvent);
    }

    // 2. Scenariusz: Przekroczenie prędkości
    private void triggerSpeedingScenario() {
        String plate = "WA-SPEED1";
        long now = Instant.now().toEpochMilli();

        sendGantryScanEvent(GantryScanEvent.builder()
                        .gantryId("GANTRY-A")
                        .plateNumber(plate)
                        .timestamp(now)
                        .build());

        // Skan na Bramce B chwilę później (ta sama rejestracja!)
        new Thread(() -> {
            try {
                Thread.sleep(3000); // 3 sekundy później
                sendGantryScanEvent(GantryScanEvent.builder()
                        .gantryId("GANTRY-B")
                        .plateNumber(plate)
                        .timestamp(Instant.now().toEpochMilli())
                        .build());
                System.out.println("🔥 [SCENARIUSZ] Wysyłano sekwencję przekroczenia prędkości dla: " + plate);
            } catch (InterruptedException ignored) {}
        }).start();
    }

    // 3. Scenariusz: Skradzione auto
    private void triggerStolenCarScenario() {
        String plate = "WA-STOLEN1"; // Tablica zgodna z tą wysłaną w StolenCarInitializer
        long now = Instant.now().toEpochMilli();
        sendGantryScanEvent(GantryScanEvent.builder()
                        .gantryId("GANTRY-A")
                        .plateNumber(plate)
                        .timestamp(now)
                        .build());
        System.out.println("🚨 [SCENARIUSZ] Wygenerowano skan skradzionego auta: " + plate);
    }

    // 4. Scenariusz: Korek na bramce
    private void triggerTrafficJamScenario() {
        System.out.println("⚠️ [SCENARIUSZ] Generowanie serii skanów (korek) na GANTRY-C...");
        for (int i = 0; i < 20; i++) {
            String plate = "KR-" + (10000 + random.nextInt(90000));
            long now = Instant.now().toEpochMilli();
            sendGantryScanEvent(GantryScanEvent.builder()
                        .gantryId("GANTRY-C")
                        .plateNumber(plate)
                        .timestamp(now)
                        .build());
        }
    }
}
