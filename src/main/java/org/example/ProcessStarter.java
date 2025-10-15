package org.example;

import io.camunda.zeebe.client.ZeebeClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class ProcessStarter implements CommandLineRunner {

    @Autowired
    private ZeebeClient zeebeClient;

    @Override
    public void run(String... args) throws Exception {
        String processId = "Process1";
        zeebeClient.newCreateInstanceCommand()
                .bpmnProcessId(processId)
                .latestVersion()
                .send()
                .join();

        System.out.println("Started a new process instance with ID: " + processId);
    }
}