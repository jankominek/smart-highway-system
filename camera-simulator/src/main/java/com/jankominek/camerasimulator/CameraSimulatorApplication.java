package com.jankominek.camerasimulator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class CameraSimulatorApplication {

    public static void main(String[] args) {
        SpringApplication.run(CameraSimulatorApplication.class, args);
    }

}
