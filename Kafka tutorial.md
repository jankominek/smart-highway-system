# Kafka tutorial

## Spis treści

1. Cel projektu
2. Architektura systemu
3. Najważniejsze pojęcia Kafka
4. Moduł `highway-contracts`
5. Wiadomości i serializacja
6. Producer i topic wejściowy
7. Kafka Streams i topologia
8. `KStream`
9. `KTable`
10. Klucze, partycje i repartition
11. State store
12. Wykrywanie przekroczenia prędkości
13. Wykrywanie skradzionego pojazdu
14. `join` w Kafka Streams
15. Wykrywanie korka przez okna czasowe
16. Rozdzielanie przepływu na gałęzie
17. Topici biznesowe i techniczne
18. Notification service
19. Tworzenie topiców
20. Testowanie i debugowanie
21. Najczęstsze błędy
22. Przydatne operacje `KStream`
23. Przydatne operacje `KTable`
24. Checklista uruchomienia
25. Słownik pojęć

---

## 1. Cel projektu

Projekt symuluje system autostradowy:

- kamery wysyłają skany pojazdów,
- procesor analizuje przejazdy,
- system wykrywa przekroczenia prędkości,
- system może rozpoznać skradziony pojazd,
- system wykrywa korek na bramce,
- notification service wypisuje czytelne powiadomienia.

Podstawowym zdarzeniem jest skan pojazdu:

```java
GantryScanEvent(
    gantryId,
    plateNumber,
    timestamp
)
```

Przykład:

```text
GANTRY-A, WA-SPEED1, 1789487262438
```

Wspólnym formatem powiadomień jest:

```java
HighwayAlert(
    id,
    type,
    plateNumber,
    gantryId,
    detectedAt,
    measuredSpeedKmh,
    vehicleCount,
    message
)
```

Typy alertów:

```java
NORMAL_TRAFFIC
SPEEDING
STOLEN_VEHICLE
TRAFFIC_JAM
```

---

## 2. Architektura systemu

Projekt składa się z kilku modułów:

```text
camera-simulator
        |
        v
camera-simulator topic
        |
        v
highway-processor
        |
        +--> speeding-alerts
        |
        +--> stolen-vehicles
        |
        +--> traffic-jam-alerts
        |
        +--> highway-alerts
                    |
                    v
            notification-service
```

### `camera-simulator`

Jest producerem. Tworzy obiekty `GantryScanEvent` i publikuje je do topicu `camera-simulator`.

### `highway-contracts`

Zwykła biblioteka JAR zawierająca wspólne klasy DTO:

- `GantryScanEvent`,
- `HighwayAlert`,
- `StolenVehicle`,
- `AlertType`.

Nie jest osobną aplikacją i nie powinna uruchamiać Spring Boota.

### `highway-processor`

Jest aplikacją Kafka Streams. Czyta skany, przetwarza je i publikuje wyniki.

### `notification-service`

Jest zwykłym konsumentem Kafka. Czyta `HighwayAlert` i wypisuje powiadomienia w konsoli.

---

## 3. Najważniejsze pojęcia Kafka

### Broker

Broker to serwer Kafka. W lokalnym projekcie broker jest dostępny dla aplikacji przez:

```text
localhost:9092
```

Jeśli komenda wykonywana jest wewnątrz kontenera Dockera, może być potrzebny:

```text
localhost:29092
```

### Topic

Topic to nazwany strumień wiadomości.

Przykłady:

```text
camera-simulator
speeding-alerts
stolen-vehicles
traffic-jam-alerts
highway-alerts
```

Wiadomość pozostaje w topicu po odczytaniu. Konsument nie usuwa jej tylko dlatego, że ją przeczytał.

### Partition

Topic może mieć jedną lub wiele partycji. Każda wiadomość trafia do konkretnej partycji.

Kafka zachowuje kolejność tylko w obrębie jednej partycji.

### Key

Klucz decyduje o partycji. Dla tego samego klucza Kafka zwykle kieruje rekordy do tej samej partycji.

W systemie kluczem dla przetwarzania pojazdu powinien być numer rejestracyjny:

```text
WA-SPEED1
```

### Offset

Offset to numer pozycji wiadomości w partycji.

Offset nie oznacza, że wiadomość zniknęła. Consumer group zapamiętuje tylko, do którego miejsca przeczytała topic.

### Consumer group

Consumer group to grupa konsumentów, która wspólnie czyta topic.

W tym projekcie:

```text
notification-service
```

jest identyfikatorem grupy consumera.

Jeżeli uruchomisz ponownie aplikację z tą samą grupą, Kafka zwykle wznowi czytanie od zapisanego offsetu.

### `auto-offset-reset`

Najczęstsze wartości:

```yaml
auto-offset-reset: earliest
```

oznacza: jeśli grupa nie ma jeszcze offsetu, czytaj od początku.

```yaml
auto-offset-reset: latest
```

oznacza: czytaj tylko nowe wiadomości.

Ta opcja nie resetuje istniejących offsetów grupy.

---

## 4. Moduł `highway-contracts`

Wspólne DTO powinny znajdować się w osobnym module, żeby producer, processor i consumer używały tego samego modelu.

Przykładowy kontrakt skanu:

```java
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class GantryScanEvent {
    private String gantryId;
    private String plateNumber;
    private long timestamp;
}
```

Przykładowy kontrakt alertu:

```java
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
}
```

Moduły korzystające z kontraktów muszą mieć zależność Maven:

```xml
<dependency>
    <groupId>com.jankominek</groupId>
    <artifactId>highway-contracts</artifactId>
    <version>0.0.1-SNAPSHOT</version>
</dependency>
```

Po zmianie kontraktu trzeba zbudować i zainstalować bibliotekę albo zbudować moduły w odpowiedniej kolejności.

---

## 5. Wiadomości i serializacja

Kafka przechowuje bajty. Aplikacja musi wiedzieć, jak zamienić obiekt na bajty i odwrotnie.

### Serializer

Serializer zamienia obiekt Java na dane wysyłane do Kafka.

### Deserializer

Deserializer zamienia dane z Kafka na obiekt Java.

### Serde

Serde to połączenie:

```text
Serializer + Deserializer
```

Dla JSON można używać:

```java
JacksonJsonSerde<GantryScanEvent> scanSerde =
        new JacksonJsonSerde<>(GantryScanEvent.class);
```

Przykład konfiguracji streamu:

```java
builder.stream(
        "camera-simulator",
        Consumed.with(
                Serdes.String(),
                scanSerde
        )
);
```

Przykład konfiguracji outputu:

```java
stream.to(
        "speeding-alerts",
        Produced.with(
                Serdes.String(),
                alertSerde
        )
);
```

Klucz i wartość muszą mieć zgodne serializery po obu stronach.

---

## 6. Producer i topic wejściowy

Producer wysyła rekord:

```text
key   = WA-SPEED1
value = GantryScanEvent
```

W symulatorze scenariusz prędkości może wyglądać tak:

```text
GANTRY-A, WA-SPEED1, t
GANTRY-B, WA-SPEED1, t + 3000 ms
GANTRY-C, WA-SPEED1, t + 6000 ms
```

Dla timestampów trzy sekundy oznaczają:

```text
3 sekundy między bramkami
```

Producer nie musi czekać trzy sekundy. Może wysłać od razu trzy rekordy z różnymi timestampami. Procesor oblicza czas na podstawie pól zdarzenia.

Jeśli chcesz symulować faktyczny upływ czasu, używasz `Thread.sleep`, schedulera albo zaplanowanych wysyłek. Do testów deterministycznych lepsze są różne timestampy bez opóźnienia.

---

## 7. Kafka Streams i topologia

Kafka Streams nie jest zwykłym `@KafkaListener`.

`@KafkaListener`:

```text
czytaj wiadomość -> wykonaj metodę
```

Kafka Streams:

```text
zdefiniuj ciąg operacji -> Kafka Streams zbuduje topologię
```

Przykład:

```java
KStream<String, GantryScanEvent> scans =
        builder.stream(
                properties.inputTopic(),
                Consumed.with(
                        Serdes.String(),
                        scanSerde
                )
        );
```

Powyższy kod nie wykonuje jeszcze logiki biznesowej. Tworzy źródło topologii.

Następnie można dodać operacje:

```java
scans
        .filter(...)
        .selectKey(...)
        .groupByKey(...)
        .windowedBy(...)
        .count();
```

Kafka Streams tworzy i uruchamia wewnętrzne procesory, taski, state store oraz topicami pomocniczymi.

### Bean topologii

W Spring Kafka topologię zwykle definiuje się w beanie:

```java
@Bean
public KStream<String, HighwayAlert> speedingStream(
        StreamsBuilder builder,
        HighwayProperties properties
) {
    // definicja topologii
}
```

`@EnableKafkaStreams` uruchamia infrastrukturę Kafka Streams.

`application.id` identyfikuje aplikację Streams:

```yaml
spring:
  kafka:
    streams:
      application-id: highway-processor
```

Nie należy zmieniać `application.id` przy każdym restarcie, jeśli chcesz zachować stan.

---

## 8. `KStream`

`KStream` reprezentuje nieograniczony strumień zdarzeń.

Przykład:

```text
scan A
scan B
scan C
scan A
...
```

Każdy rekord jest osobnym zdarzeniem. `KStream` nie reprezentuje tylko bieżącego stanu.

### `filter`

Usuwa rekordy, które nie spełniają warunku:

```java
validScans = scans.filter((key, event) ->
        event != null
                && event.getPlateNumber() != null
                && !event.getPlateNumber().isBlank()
);
```

### `mapValues`

Zmienia wartość, zachowując klucz:

```java
alerts = scans.mapValues(event ->
        HighwayAlert.fromGantryScanEvent(
                event,
                "Normal traffic",
                AlertType.NORMAL_TRAFFIC
        )
);
```

### `map`

Zmienia jednocześnie klucz i wartość:

```java
stream.map((key, value) ->
        KeyValue.pair(
                value.getPlateNumber(),
                newValue
        )
);
```

### `selectKey`

Zmienia klucz logiczny:

```java
scans.selectKey((ignoredKey, event) ->
        event.getPlateNumber()
);
```

Stary klucz jest ignorowany, a nowym jest numer rejestracyjny.

### `peek`

Służy do obserwacji rekordów bez zmiany przepływu:

```java
stream.peek((key, value) ->
        System.out.println("Received: " + value)
);
```

`peek` jest dobry do debugowania, ale nie powinien zawierać ważnej logiki biznesowej.

### `to`

Zapisuje strumień do topicu:

```java
alerts.to(
        "speeding-alerts",
        Produced.with(
                Serdes.String(),
                alertSerde
        )
);
```

### `branch`

Rozdziela strumień na kilka gałęzi:

```java
Map<String, KStream<String, Event>> branches =
        stream.split(Named.as("traffic-"))
                .branch(
                        (key, event) -> event.isStolen(),
                        Branched.as("stolen")
                )
                .branch(
                        (key, event) -> event.isSpeeding(),
                        Branched.as("speeding")
                )
                .defaultBranch(Branched.as("normal"));
```

W praktyce kilka osobnych `filter` również tworzy logiczne gałęzie.

---

## 9. `KTable`

`KTable` reprezentuje aktualny stan według klucza.

Przykład:

```text
WA-12345 -> stolen=true
```

Jeśli przyjdzie aktualizacja:

```text
WA-12345 -> stolen=false
```

to `KTable` przechowuje już:

```text
WA-12345 -> stolen=false
```

`KStream` mówi:

```text
co się wydarzyło
```

`KTable` mówi:

```text
jaki jest obecny stan
```

### Utworzenie tabeli z topicu

```java
KTable<String, StolenVehicle> stolenRegistry =
        builder.table(
                "stolen-vehicle-registry",
                Consumed.with(
                        Serdes.String(),
                        stolenVehicleSerde
                )
        );
```

Kafka Streams odczytuje topic i utrzymuje lokalny stan tabeli.

### `filter` na `KTable`

```java
KTable<String, StolenVehicle> stolenOnly =
        stolenRegistry.filter((plate, vehicle) ->
                vehicle != null && vehicle.stolen()
        );
```

Teraz tabela zawiera tylko rekordy z `stolen=true`.

### `toStream`

Aktualizacje tabeli można zamienić na strumień:

```java
KStream<String, Long> updates =
        counts.toStream();
```

Jest to przydatne, gdy chcesz wysłać zmianę stanu do topicu.

---

## 10. Klucze, partycje i repartition

Kafka Streams wykonuje większość operacji według klucza.

### Dlaczego klucz pojazdu jest ważny?

Wykrywanie prędkości wymaga wszystkich skanów jednego pojazdu w jednym tasku:

```text
WA-SPEED1 -> GANTRY-A
WA-SPEED1 -> GANTRY-B
WA-SPEED1 -> GANTRY-C
```

Dlatego przed processorami:

```java
scansByPlate = scans
        .selectKey((ignoredKey, event) ->
                event.getPlateNumber()
        )
        .repartition(
                Repartitioned.with(
                        Serdes.String(),
                        scanSerde
                )
        );
```

### `selectKey` a `repartition`

`selectKey` zmienia klucz w rekordzie logicznie.

`repartition` fizycznie przesyła rekordy do partycji zgodnie z nowym kluczem.

Bez repartition rekord może nadal znajdować się w partycji wybranej według starego klucza.

### `groupByKey`

```java
stream.groupByKey(
        Grouped.with(
                Serdes.String(),
                scanSerde
        )
);
```

`groupByKey` grupuje rekordy po aktualnym kluczu i w razie potrzeby tworzy wewnętrzny topic repartition.

### Najczęstszy błąd

Nie grupuj korka po numerze rejestracyjnym. Dla korka potrzebujesz:

```java
.selectKey((ignoredKey, event) ->
        event.getGantryId()
)
```

Dla prędkości potrzebujesz:

```java
.selectKey((ignoredKey, event) ->
        event.getPlateNumber()
)
```

---

## 11. State store

State store jest lokalnym magazynem stanu aplikacji Kafka Streams.

Może przechowywać na przykład:

```text
WA-SPEED1 -> ostatni skan pojazdu
```

Dla prostego procesora używa się:

```java
KeyValueStore<String, GantryScanEvent>
```

### Rejestracja state store

```java
builder.addStateStore(
        Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(
                        "last-scan-by-plate"
                ),
                Serdes.String(),
                scanSerde
        )
);
```

### Odczyt w Processor API

```java
lastScans = (KeyValueStore<String, GantryScanEvent>)
        context.getStateStore("last-scan-by-plate");
```

### Zapis i odczyt

```java
GantryScanEvent previous =
        lastScans.get(plateNumber);

lastScans.put(
        plateNumber,
        current
);
```

State store jest lokalny, ale Kafka Streams tworzy dla niego changelog topic. Dzięki temu po restarcie stan może zostać odtworzony.

### State store a topic changelog

Wewnętrzny changelog może mieć nazwę podobną do:

```text
highway-processor-last-scan-by-plate-changelog
```

Nie jest to topic biznesowy dla innych serwisów.

---

## 12. Wykrywanie przekroczenia prędkości

Założenie:

```text
GANTRY-A -> GANTRY-B = 0.5 km
limit = 120 km/h
czas przejazdu = 3 sekundy
```

Wzór:

```text
czas w godzinach = elapsedMillis / 3_600_000.0
prędkość = distanceKm / czas w godzinach
```

Dla 0.5 km i 3 sekund:

```text
0.5 / (3000 / 3_600_000)
= 600 km/h
```

To oznacza przekroczenie limitu.

### Kolejność procesora

1. Pobierz aktualny skan.
2. Pobierz poprzedni skan dla rejestracji.
3. Zapisz aktualny skan jako ostatni.
4. Jeśli nie ma poprzedniego, zakończ.
5. Oblicz różnicę czasu.
6. Sprawdź maksymalny interwał.
7. Zbuduj trasę, np. `GANTRY-A->GANTRY-B`.
8. Pobierz dystans dla trasy.
9. Oblicz prędkość.
10. Porównaj z limitem.
11. Jeśli limit przekroczony, wyślij `HighwayAlert`.

### Konfiguracja tras

Procesor buduje dokładnie takie klucze:

```text
GANTRY-A->GANTRY-B
GANTRY-B->GANTRY-C
```

Konfiguracja musi odpowiadać dokładnie:

```yaml
distances-km:
  "[GANTRY-A->GANTRY-B]": 0.5
  "[GANTRY-B->GANTRY-C]": 0.5
```

Nawiasy kwadratowe pomagają Spring Boot zachować znaki specjalne w kluczu mapy.

Jeśli konfiguracja ma:

```yaml
GANTRYAB: 0.5
GANTRYBC: 0.5
```

to lookup dla `GANTRY-A->GANTRY-B` zwróci `null`.

### Dlaczego może nie powstać alert?

Każdy z tych warunków może zakończyć proces:

```java
previous == null
elapsedMillis <= 0
elapsedMillis > maxInterval
distanceKm == null
speedKmh <= speedLimit
```

W czasie debugowania warto logować:

```text
previous
current
elapsedMillis
route
distanceKm
speedKmh
```

---

## 13. Wykrywanie skradzionego pojazdu

### Wersja uproszczona

Jeżeli reguła testowa mówi, że rejestracja zawierająca `STOLEN` oznacza kradzież, wystarczy:

```java
scansByPlate
        .filter((plate, event) ->
                plate.contains("STOLEN")
        )
        .map((plate, event) ->
                new KeyValue<>(
                        plate,
                        HighwayAlert.fromGantryScanEvent(
                                event,
                                "Stolen vehicle detected",
                                AlertType.STOLEN_VEHICLE
                        )
                )
        )
        .to(
                "stolen-vehicles",
                Produced.with(
                        Serdes.String(),
                        alertSerde
                )
        );
```

Do takiej reguły nie potrzeba `join`, bo informacja znajduje się w samym skanie.

### Wersja realistyczna

W prawdziwym systemie kamera nie powinna sama decydować, czy pojazd jest skradziony. Kamera wysyła tablicę, a osobny rejestr zawiera status.

```text
camera-simulator:
WA-12345 -> GantryScanEvent

stolen-vehicle-registry:
WA-12345 -> StolenVehicle(stolen=true)
```

Wtedy używa się `KStream-KTable join`.

---

## 14. `join` w Kafka Streams

`join` oznacza połączenie danych z dwóch źródeł po kluczu.

### Co jest kluczem?

Join porównuje:

```java
stream.key() == table.key()
```

Nie porównuje automatycznie pól wewnątrz JSON-a.

Poprawne rekordy:

```text
stream key: WA-12345
table key:  WA-12345
```

### Tabela rejestru

```java
KTable<String, StolenVehicle> registry =
        builder.table(
                "stolen-vehicle-registry",
                Consumed.with(
                        Serdes.String(),
                        stolenVehicleSerde
                )
        );
```

### Zostawienie tylko skradzionych pojazdów

Sam `join` nie sprawdza `stolen=true`. Najpierw można odfiltrować tabelę:

```java
KTable<String, StolenVehicle> stolenOnly =
        registry.filter((plate, vehicle) ->
                vehicle != null && vehicle.stolen()
        );
```

### Join skanu z tabelą

```java
KStream<String, HighwayAlert> stolenAlerts =
        scansByPlate.join(
                stolenOnly,
                (scan, stolenVehicle) ->
                        HighwayAlert.fromGantryScanEvent(
                                scan,
                                "Stolen vehicle detected",
                                AlertType.STOLEN_VEHICLE
                        )
        );
```

### Co dzieje się pod spodem?

1. Topic rejestru jest materializowany jako lokalna mapa.
2. Do tabeli trafia najnowszy status dla każdej tablicy.
3. Przychodzi skan z kluczem `WA-12345`.
4. Kafka Streams szuka `WA-12345` w lokalnej tabeli.
5. Jeśli wpis istnieje, wywoływana jest funkcja łącząca.
6. Powstaje wynikowy `HighwayAlert`.

### Inner join

```java
scans.join(registry, joiner)
```

Jeśli w tabeli nie ma klucza, nie powstaje wynik.

### Left join

```java
scans.leftJoin(registry, joiner)
```

Skan może zostać zachowany również wtedy, gdy nie ma dopasowania. Druga wartość będzie wtedy `null`.

### Ważna kolejność

`KStream-KTable join` sprawdza aktualny stan tabeli w momencie nadejścia skanu.

Jeśli skan przyjdzie przed wpisem o kradzieży, późniejsza zmiana tabeli nie musi automatycznie wygenerować alertu dla starego skanu.

To nie jest zapytanie SQL przeszukujące całą historię topicu.

---

## 15. Wykrywanie korka przez okna czasowe

Założenie:

```text
na jednej bramce
w ciągu jednej minuty
pojawiło się co najmniej 15 pojazdów
```

### Zmiana klucza na bramkę

```java
validScans.selectKey((ignoredKey, event) ->
        event.getGantryId()
);
```

Przykład:

```text
GANTRY-C -> WA-10001
GANTRY-C -> WA-10002
GANTRY-C -> WA-10003
```

### Grupowanie

```java
.groupByKey(
        Grouped.with(
                Serdes.String(),
                scanSerde
        )
)
```

Wszystkie skany jednej bramki trafiają do tej samej grupy.

### Okno

```java
.windowedBy(
        TimeWindows.ofSizeAndGrace(
                properties.trafficJam().window(),
                properties.trafficJam().gracePeriod()
        )
)
```

Przykładowa konfiguracja:

```yaml
traffic-jam:
  window: 1m
  grace-period: 10s
  threshold: 15
```

### Liczenie

```java
.count()
```

Rezultatem jest:

```java
KTable<Windowed<String>, Long>
```

`Windowed<String>` zawiera bramkę oraz przedział czasu.

### Przepływ przykładowy

```text
GANTRY-C, 12:00:01 -> count 1
GANTRY-C, 12:00:10 -> count 2
GANTRY-C, 12:00:30 -> count 15
```

Po osiągnięciu progu można utworzyć alert.

### `toStream`

```java
vehiclesByGantry.toStream()
```

`count()` tworzy `KTable`, a `toStream()` zamienia aktualizacje tabeli na rekordy strumienia.

### Próg

```java
.filter((windowedGantry, count) ->
        count >= threshold
)
```

Może wygenerować aktualizacje dla `15`, `16`, `17` itd.

### Jeden alert na okno

Jeśli chcesz wysłać jeden końcowy alert:

```java
KTable<Windowed<String>, Long> completedCounts =
        vehiclesByGantry.suppress(
                Suppressed.untilWindowCloses(
                        Suppressed.BufferConfig.unbounded()
                )
        );
```

Następnie filtrujesz:

```java
completedCounts
        .toStream()
        .filter((windowedGantry, count) ->
                count >= threshold
        );
```

W takim rozwiązaniu alert zostanie wysłany po zamknięciu okna i grace period, a nie dokładnie w momencie 15. skanu.

### Liczba rekordów a liczba samochodów

Zwykłe `count()` liczy rekordy. Jeśli ten sam samochód pojawi się trzy razy, zostanie policzony trzy razy.

Jeśli chcesz liczyć unikalne tablice, potrzebujesz agregacji zbioru, np.:

```text
GANTRY-C -> Set<String> plateNumbers
```

To zajmuje więcej pamięci, ale lepiej odpowiada rzeczywistej liczbie samochodów.

---

## 16. Rozdzielanie przepływu na gałęzie

Jeden strumień wejściowy może mieć wielu odbiorców:

```text
validScans
    |
    +--> normal traffic
    |
    +--> speeding processor
    |
    +--> stolen filter
    |
    +--> traffic jam aggregation
```

Nie trzeba tworzyć osobnej aplikacji dla każdej gałęzi.

### Aktualny model topiców

Wszystkie wiadomości wynikowe są typu `HighwayAlert`:

```text
highway-alerts      -> NORMAL_TRAFFIC
speeding-alerts     -> SPEEDING
stolen-vehicles     -> STOLEN_VEHICLE
traffic-jam-alerts  -> TRAFFIC_JAM
```

Dzięki temu notification service potrzebuje jednego deserializera i jednego listenera.

Nie powinno się mieszać w jednym topicu:

```text
GantryScanEvent
HighwayAlert
```

również wtedy, gdy oba obiekty są JSON-em.

---

## 17. Topici biznesowe i techniczne

### Topici biznesowe

Są częścią kontraktu systemu i mogą być czytane przez inne serwisy:

```text
camera-simulator
speeding-alerts
stolen-vehicles
traffic-jam-alerts
highway-alerts
```

### Topici techniczne

Są tworzone przez Kafka Streams:

```text
REPARTITION
CHANGELOG
```

Nie należy ręcznie budować logiki biznesowej na podstawie ich nazw.

### Tworzenie topiców przez Spring

Można centralnie utworzyć topici:

```java
@Bean
public KafkaAdmin.NewTopics kafkaTopics() {
    return new KafkaAdmin.NewTopics(
            TopicBuilder.name("camera-simulator")
                    .partitions(1)
                    .replicas(1)
                    .build(),
            TopicBuilder.name("speeding-alerts")
                    .partitions(1)
                    .replicas(1)
                    .build()
    );
}
```

Kafka utworzy topic, jeśli nie istnieje.

W lokalnym środowisku `replicas(1)` jest wystarczające. W produkcji liczba replik powinna odpowiadać liczbie brokerów i wymaganiom niezawodności.

---

## 18. Notification service

Notification service używa zwykłego `@KafkaListener`.

Przykład:

```java
@KafkaListener(
        topics = {
                "speeding-alerts",
                "stolen-vehicles",
                "traffic-jam-alerts",
                "highway-alerts"
        },
        groupId = "notification-service"
)
public void receiveAlert(HighwayAlert alert) {
    log.info("{}", formatAlert(alert));
}
```

Wspólny deserializer może obsłużyć wszystkie topici, ponieważ wszystkie zawierają `HighwayAlert`.

Logowanie może używać ikon:

```text
🟢 NORMALNY RUCH
‼️ PRZEKROCZENIE PRĘDKOŚCI
🚓 SKRADZIONY POJAZD
🚧 KOREK
```

Jeśli topic zawierał wcześniej inny format JSON, stare wiadomości nadal mają stary format. Kafka nie przepisuje historii po zmianie kodu.

Możliwe rozwiązania:

- użyć nowego topicu,
- użyć nowej grupy konsumenta,
- wyczyścić lokalny topic,
- zostawić stare rekordy i czytać tylko nowe.

---

## 19. Tworzenie topiców

Są trzy różne mechanizmy:

### Kafka Streams tworzy topici wewnętrzne

Dotyczy:

```text
repartition
changelog
```

### Spring Kafka tworzy topici biznesowe

Dotyczy beanów `NewTopic` i `NewTopics`.

### Broker może automatycznie tworzyć topic

To zależy od konfiguracji brokera. W projekcie edukacyjnym lepiej jawnie definiować ważne topici.

---

## 20. Testowanie i debugowanie

### Sprawdzenie, czy aplikacja działa

Kafka Streams powinno dojść do stanu:

```text
RUNNING
```

Wcześniejsze stany:

```text
CREATED
REBALANCING
PARTITIONS_ASSIGNED
```

są normalne podczas startu.

### `Lag: 0`

`Lag: 0` oznacza, że consumer przeczytał wszystkie dostępne rekordy do końca. Nie oznacza, że topic jest pusty.

### Debugowanie przepływu

Loguj kolejne punkty:

```text
Received scan
Filtered scan
After repartition
Inside processor
Speeding alert
Stolen alert
Traffic jam count
```

Jeśli widać `Filtered scan`, ale nie ma alertu, trzeba sprawdzić warunki wewnątrz procesora.

### Sprawdzanie topicu

Przykładowo:

```bash
docker exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:29092 \
  --topic speeding-alerts \
  --from-beginning \
  --property print.key=true
```

W lokalnym dashboardzie można obserwować:

- wiadomości,
- klucze,
- partycje,
- offsety,
- consumer groups.

### Gdy nic nie trafia do outputu

Sprawdź kolejno:

1. Czy topic wejściowy istnieje?
2. Czy producer wysyła rekordy?
3. Czy rekord przechodzi `filter`?
4. Czy ma właściwy klucz?
5. Czy processor jest wywoływany?
6. Czy state store zwraca poprzednią wartość?
7. Czy konfiguracja mapy pasuje do wygenerowanej trasy?
8. Czy output `.to(...)` jest podłączony?
9. Czy konsument używa właściwego deserializera?
10. Czy obserwujesz właściwy topic?

---

## 21. Najczęstsze błędy

### `MissingSourceTopicException`

Topic wejściowy nie istnieje.

Rozwiązanie:

- utwórz topic,
- dodaj `NewTopic`,
- sprawdź literówkę w nazwie.

### `NoClassDefFoundError: TypeReference`

Brakuje Jacksona w aplikacji używającej `JsonDeserializer`.

Dodaj:

```xml
<dependency>
    <groupId>com.fasterxml.jackson.core</groupId>
    <artifactId>jackson-databind</artifactId>
</dependency>
```

### `Cannot convert ...`

Listener oczekuje innego typu niż deserializer lub producer.

Przykład:

```text
consumer oczekuje GantryScanEvent
deserializer zwraca HighwayAlert
```

Najprostsze rozwiązanie to jeden kontrakt na danym topicu.

### Brak `Speeding alert`

Najczęstsze powody:

- `previous == null`,
- zły timestamp,
- `elapsedMillis` poza zakresem,
- brak dystansu dla trasy,
- prędkość nie przekracza limitu,
- procesor zapisuje stan, ale nie wykonuje `context.forward`.

### Zła nazwa trasy

Procesor tworzy:

```text
GANTRY-A->GANTRY-B
```

Konfiguracja musi mieć identyczny klucz.

### Stare topici repartition

Po zmianach topologii mogą zostać stare topici techniczne. Nie zawsze oznacza to błąd. Kafka Streams używa topiców wynikających z aktualnego `application.id` i topologii.

### Zmiana `application.id`

Nowe `application.id` oznacza nową aplikację Streams i nowy stan. Poprzedni state store nie zostanie użyty tak jak wcześniej.

### `Process finished with exit code 130`

Kod 130 oznacza przerwanie sygnałem `SIGINT`, zwykle przez:

- `Ctrl+C`,
- przycisk Stop w IDE.

Nie jest to błąd kodu.

---

## 22. Przydatne operacje `KStream`

### `filter`

Zostawia wybrane rekordy:

```java
stream.filter((key, value) -> condition);
```

### `map`

Zmienia klucz i wartość:

```java
stream.map((key, value) ->
        KeyValue.pair(newKey, newValue)
);
```

### `mapValues`

Zmienia tylko wartość:

```java
stream.mapValues(value -> newValue);
```

### `selectKey`

Ustawia nowy klucz:

```java
stream.selectKey((key, value) -> value.getPlateNumber());
```

### `flatMap`

Jeden rekord może zamienić się w wiele rekordów:

```java
stream.flatMap((key, value) ->
        List.of(
                KeyValue.pair("A", value),
                KeyValue.pair("B", value)
        )
);
```

### `groupByKey`

Grupuje po istniejącym kluczu:

```java
stream.groupByKey(grouped);
```

### `groupBy`

Najpierw wybiera nowy klucz i grupuje:

```java
stream.groupBy(
        (key, value) -> value.getGantryId(),
        grouped
);
```

### `join`

Łączy z innym strumieniem lub tabelą.

### `leftJoin`

Łączy, ale zachowuje również rekordy bez dopasowania.

### `merge`

Łączy dwa strumienie tego samego typu:

```java
streamA.merge(streamB);
```

### `branch`

Rozdziela strumień według warunków.

### `process`

Podłącza własny Processor API:

```java
stream.process(
        new SpeedingProcessorSupplier(properties),
        SpeedingProcessor.LAST_SCAN_STORE
);
```

### `transform`

Pozwala użyć własnego transformera i state store. Jest przydatne przy bardziej zaawansowanych regułach.

### `to`

Kończy gałąź zapisem do topicu.

---

## 23. Przydatne operacje `KTable`

### `filter`

Filtruje bieżący stan:

```java
table.filter((key, value) ->
        value != null && value.isActive()
);
```

### `mapValues`

Zmienia wartości tabeli:

```java
table.mapValues(value -> newValue);
```

### `toStream`

Zamienia aktualizacje tabeli na strumień:

```java
table.toStream();
```

### `join`

Łączy dwie tabele po tym samym kluczu:

```java
leftTable.join(
        rightTable,
        (left, right) -> combine(left, right)
);
```

### `leftJoin`

Łączy tabele i zachowuje rekordy, dla których prawa strona może nie istnieć.

### `groupBy`

Zmienia klucz i typ grupowania tabeli, zwykle przed agregacją.

---

## 24. Testowanie topologii

Do testów Kafka Streams można użyć `TopologyTestDriver`.

Schemat:

1. Utwórz `StreamsBuilder`.
2. Zbuduj topologię.
3. Utwórz `TopologyTestDriver`.
4. Dodaj input topic.
5. Wyślij rekordy testowe.
6. Odczytaj output topic.
7. Sprawdź alerty.

Test prędkości:

```text
A at t
B at t + 3000 ms
C at t + 6000 ms
```

Oczekiwany wynik:

```text
A -> B: alert
B -> C: alert
```

Test bez przekroczenia:

```text
A at t
B at t + 60 seconds
```

Oczekiwany wynik:

```text
brak alertu
```

Test korka:

```text
15 różnych tablic
ta sama bramka
to samo okno czasowe
```

Oczekiwany wynik:

```text
TRAFFIC_JAM
```

Test stolen:

```text
plateNumber = WA-STOLEN1
```

Oczekiwany wynik:

```text
STOLEN_VEHICLE
```

---

## 25. Checklista uruchomienia

### Przed uruchomieniem

- broker Kafka działa,
- topic `camera-simulator` istnieje,
- contracts jest zbudowany,
- konfiguracja bootstrap server jest poprawna,
- nazwy topiców są identyczne w producerze i consumerze,
- serwisy używają zgodnych kontraktów JSON.

### Kolejność

1. Uruchom Kafka i dashboard.
2. Uruchom `highway-processor`.
3. Sprawdź stan Kafka Streams `RUNNING`.
4. Uruchom `notification-service`.
5. Uruchom `camera-simulator`.
6. Obserwuj logi procesora.
7. Obserwuj logi notification service.

### Po zmianie modelu wiadomości

- zbuduj `highway-contracts`,
- zbuduj processor,
- zbuduj notification service,
- upewnij się, że topic nie zawiera mieszanych typów,
- rozważ nowy topic lub nową grupę konsumenta.

---

## Najważniejszy obraz całości

```text
Producer
    |
    v
KStream<key, GantryScanEvent>
    |
    +--> normalna wiadomość
    |       -> HighwayAlert(NORMAL_TRAFFIC)
    |       -> highway-alerts
    |
    +--> klucz plateNumber
    |       -> state store poprzedniego skanu
    |       -> obliczenie prędkości
    |       -> speeding-alerts
    |
    +--> filtr lub join stolen registry
    |       -> stolen-vehicles
    |
    +--> klucz gantryId
            -> okno czasowe
            -> count
            -> threshold
            -> traffic-jam-alerts

Notification service
    |
    +--> jeden HighwayAlert deserializer
    +--> jeden listener
    +--> czytelny log z ikoną
```

Najważniejsza zasada:

```text
KStream = wydarzenia
KTable = aktualny stan
state store = lokalny stan procesu
join = połączenie po kluczu
window = ograniczenie agregacji do czasu
repartition = fizyczne uporządkowanie rekordów według nowego klucza
```
