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

    private String generateLogMessage(final ActivatedJob job, String type) {
        final Map<String, Object> variables = job.getVariablesAsMap();

        return String.format("%s | Timestamp: %s | Product: %s | Quantity: %s | ProcessInstanceKey: %d%n",
                type,
                LocalDateTime.now(),
                variables.get("namaProduk"),
                variables.get("jumlah"),
                job.getProcessInstanceKey());
    }

    @JobWorker(type = "OrderSuccess")
    public void handleInformasiSukses(final ActivatedJob job) {
        String logMessage = generateLogMessage(job, "BERHASIL");

        log.info("Mengirim informasi pesanan BERHASIL ke kustomer... Writing to file.");

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss-SSS");
        String timestamp = LocalDateTime.now().format(formatter);
        String saveName = String.format("success-order-%s.txt", timestamp);

        writeToFile(saveName, logMessage);
    }

    @JobWorker(type = "OrderException")
    public void handleInformasiGagal(final ActivatedJob job) {
        String logMessage = generateLogMessage(job, "GAGAL");

        log.error("Mengirim informasi pesanan GAGAL ke kustomer... Writing to file.");

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss-SSS");
        String timestamp = LocalDateTime.now().format(formatter);
        String saveName = String.format("failed-order-%s.txt", timestamp);

        writeToFile(saveName, logMessage);
    }

    private void writeToFile(String fileName, String message) {
        try {
            Files.writeString(Paths.get(fileName), message, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            log.error("Error writing to file {}", fileName, e);
        }
    }
}