package com.rbhatiya.cabbookdriver.controller;

import com.rbhatiya.cabbookdriver.service.CabLocationService;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@RestController
@RequestMapping("location")
public class CabLocationController {

    @Autowired
    private CabLocationService cabLocationService;

    /**
     * Method to update location
     * @return
     * @throws InterruptedException
     */
    @PutMapping
    public ResponseEntity updateLocation() throws InterruptedException {
        int range = 100;
        while (range > 0) {
            //cabLocationService.updateLocation(Math.random() + " , " + Math.random());
            cabLocationService.KafkaProducerWithOffset(Math.random() + " , " + Math.random());
            Thread.sleep(1000);
            range--;
        }
        return new ResponseEntity<>(Map.of("message", "Location Updated Successfully"), HttpStatus.OK);
    }

    @PostMapping
    public ResponseEntity createLocationTopic() {
        cabLocationService.createTopic();
        return new ResponseEntity<>(Map.of("message", "Location Created Successfully"), HttpStatus.OK);
    }


}
