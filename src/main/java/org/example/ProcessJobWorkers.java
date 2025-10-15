package org.example;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;


@Component
public class ProcessJobWorkers {
    private static final Logger log = LoggerFactory.getLogger(ProcessJobWorkers.class);

    private List<Map<String, Object>> vehicles;

    @PostConstruct
    public void loadVehicles() {
        ObjectMapper objectMapper = new ObjectMapper();
        try (InputStream inputStream = new ClassPathResource("vehicles.json").getInputStream()) {
            this.vehicles = objectMapper.readValue(inputStream, new TypeReference<>() {});
            log.info("Berhasil memuat {} kendaraan dari vehicles.json", vehicles.size());
        } catch (IOException e) {
            log.error("Gagal memuat vehicles.json", e);
            this.vehicles = Collections.emptyList();
        }
    }

    @JobWorker(type = "UpdateDatabase")
    public void handleSimpanPesanan(final ActivatedJob job) {
        log.info("Menyimpan pesanan ke database... Job Key: {}", job.getKey());
        final Map<String, Object> variables = job.getVariablesAsMap();

        log.info("Order untuk produk '{}' dengan jumlah {} telah disimpan.", variables.get("namaProduk"), variables.get("jumlah"));
    }

    @JobWorker(type = "CheckInventory")
    public void handleCheckInventory(final JobClient client, final ActivatedJob job) {
        log.info("Memeriksa inventory... Job Key: {}", job.getKey());

        final Map<String, Object> variables = job.getVariablesAsMap();
        String productName = (String) variables.get("namaProduk");

        Map<String, String> headers = job.getCustomHeaders();
        String url = headers.getOrDefault("url", "https://dummyjson.com/test");
        String method = headers.getOrDefault("method", "GET");

        log.info("Memanggil mock API {} dengan {}", url, method);

        try {
            HttpClient httpClient = HttpClient.newHttpClient();

            String body = String.format("{\"product\": \"%s\"}", productName);
            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Content-Type", "application/json");

            if ("POST".equalsIgnoreCase(method)) {
                requestBuilder.POST(HttpRequest.BodyPublishers.ofString(body));
            } else {
                requestBuilder.GET();
            }

            HttpRequest request = requestBuilder.build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            log.info("Mock API response status: {} and body: {}", response.statusCode(), response.body());
        } catch (Exception e) {
            log.warn("Mock API gagal: {}", e.getMessage());
        }

        boolean itemAvailable = true;
        client.newCompleteCommand(job.getKey())
                .variables(Map.of("itemAvailable", itemAvailable))
                .send()
                .join();
    }

    @JobWorker(type = "GetKendaraanPengangkut")
    public void handlePenentuanKendaraan(final JobClient client, final ActivatedJob job) {
        log.info("Menentukan kendaraan pengangkut... Job Key: {}", job.getKey());

        final Map<String, Object> variables = job.getVariablesAsMap();
        final String jumlahStr = (String) variables.get("jumlah");
        int jumlah;

        try {
            jumlah = Integer.parseInt(jumlahStr);
        } catch (NumberFormatException e) {
            log.error("Jumlah tidak valid. Tidak dapat di-parse menjadi integer: {}", jumlahStr);

            client.newFailCommand(job.getKey())
                    .retries(0)
                    .errorMessage("Jumlah tidak valid: " + jumlahStr)
                    .send().join();
            return;
        }

        log.info("Mencari kendaraan untuk mengangkut sejumlah {}", jumlah);
        Optional<Map<String, Object>> availableVehicle = vehicles.stream()
                .filter(v -> (Integer) v.get("capacity") >= jumlah)
                .findFirst();

        boolean isAvailable = availableVehicle.isPresent();
        String vehicleId = isAvailable ? (String) availableVehicle.get().get("id") : null;
        if (isAvailable) {
            log.info("Kendaraan ditemukan: ID {}", vehicleId);
        } else {
            log.warn("Tidak ada kendaraan yang tersedia untuk jumlah {}", jumlah);
        }

        Map<String, Object> retVars = new HashMap<>();
        retVars.put("kendaraanAvailable", isAvailable);
        retVars.put("vehicleId", vehicleId);

        client.newCompleteCommand(job.getKey())
                .variables(retVars)
                .send()
                .join();
    }

    @JobWorker(type = "GetDO")
    public void handleCetakDO(final ActivatedJob job) {
        log.info("Mencetak Delivery Order (DO)... Job Key: {}", job.getKey());
        final Map<String, Object> variables = job.getVariablesAsMap();
        log.info("DO untuk produk '{}' dengan jumlah {} sedang dicetak.", variables.get("namaProduk"), variables.get("jumlah"));
    }

    @JobWorker(type = "OrderSuccess")
    public void handleInformasiSukses(final ActivatedJob job) {
        handleInformation(job, true);
    }

    @JobWorker(type = "OrderException")
    public void handleInformasiGagal(final ActivatedJob job) {
        handleInformation(job, false);
    }

    // ----- HELPER FUNCTIONS ----

    private void writeToFile(String fileName, String message) {
        try {
            Files.writeString(Paths.get(fileName), message, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            log.error("Error writing to file {}", fileName, e);
        }
    }

    private String generateLogMessage(final ActivatedJob job, boolean isSuccess) {
        final Map<String, Object> variables = job.getVariablesAsMap();

        String type = isSuccess ? "SUCCESS" : "FAILED";

        return String.format("%s | Timestamp: %s | Product: %s | Quantity: %s | ProcessInstanceKey: %d%n",
                type,
                LocalDateTime.now(),
                variables.get("namaProduk"),
                variables.get("jumlah"),
                job.getProcessInstanceKey());
    }

    private String generateSaveName(boolean isSuccess) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss-SSS");
        String timestamp = LocalDateTime.now().format(formatter);

        String prefix = isSuccess ? "success" : "failed";

        return String.format("Output/%s-order-%s.txt", prefix, timestamp);
    }

    private void handleInformation(final ActivatedJob job, boolean isSuccess) {
        String logMessage = generateLogMessage(job, isSuccess);
        String saveName = generateSaveName(isSuccess);

        writeToFile(saveName, logMessage);
    }
}