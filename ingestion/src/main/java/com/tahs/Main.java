package com.tahs;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class Main {

    private static final Path CONTROL_PATH = Paths.get("../control");
    private static final Path DOWNLOADS_FILE = CONTROL_PATH.resolve("downloaded_books.txt");
    private static final String STAGING_PATH = "../staging/downloads";
    private static final int TOTAL_BOOKS = 70000;
    private static final int MAX_RETRIES = 10;

    public static void main(String[] args) {
        System.out.println("[INGESTION] Iniciando ciclo de descarga...");
        try {
            Files.createDirectories(CONTROL_PATH);
            Files.createDirectories(Paths.get(STAGING_PATH));

            Set<String> downloaded = readDownloadedIds();
            Random random = new Random();

            for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
                int candidateId = random.nextInt(TOTAL_BOOKS) + 1;
                if (downloaded.contains(String.valueOf(candidateId))) {
                    System.out.println("[INFO] Libro " + candidateId + " ya descargado, buscando otro...");
                    continue;
                }

                System.out.println("[CONTROL] Descargando nuevo libro con ID " + candidateId + "...");
                boolean ok = BookFunctions.downloadBook(candidateId, STAGING_PATH);
                if (ok) {
                    boolean datalakeOk = BookFunctions.createDatalake(candidateId, STAGING_PATH);
                    if (datalakeOk) {
                        appendDownloadedId(candidateId);
                        System.out.println("[CONTROL] Libro " + candidateId + " descargado y movido al datalake correctamente.");
                        return;
                    }
                }
            }

            System.out.println("[CONTROL] No se encontró un nuevo libro válido en este ciclo.");

        } catch (IOException e) {
            System.out.println("[ERROR] Error general en ingestion: " + e.getMessage());
        }
    }

    private static Set<String> readDownloadedIds() throws IOException {
        Set<String> ids = new HashSet<>();
        if (Files.exists(DOWNLOADS_FILE)) {
            ids.addAll(Files.readAllLines(DOWNLOADS_FILE, StandardCharsets.UTF_8));
        }
        return ids;
    }

    private static void appendDownloadedId(int id) {
        try {
            Files.createDirectories(CONTROL_PATH);
            Files.writeString(DOWNLOADS_FILE, id + System.lineSeparator(),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND);
        } catch (IOException e) {
            System.out.println("[WARN] No se pudo registrar el ID descargado " + id + ": " + e.getMessage());
        }
    }
}
