package com.tahs;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.regex.Pattern;

public class BookFunctions {

    private static final String START_MARKER = "*** START OF THE PROJECT GUTENBERG EBOOK";
    private static final String END_MARKER = "*** END OF THE PROJECT GUTENBERG EBOOK";

    /**
     * Downloads a book from Project Gutenberg and saves its header and body separately.
     *
     * @param bookId     The ID of the book to download.
     * @param outputPath The local folder to save the downloaded files.
     * @return true if download was successful, false otherwise.
     */
    @SuppressWarnings("resource")
    public static boolean downloadBook(int bookId, String outputPath) {
        try {
            Path outputDir = Paths.get(outputPath);
            if (Files.notExists(outputDir)) {
                Files.createDirectories(outputDir);
            }

            String url = String.format("https://www.gutenberg.org/cache/epub/%d/pg%d.txt", bookId, bookId);
            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() != 200) {
                System.out.println("[ERROR] HTTP " + response.statusCode() + " when downloading book " + bookId);
                return false;
            }

            String text = response.body();
            if (!text.contains(START_MARKER) || !text.contains(END_MARKER)) {
                System.out.println("[WARN] Start/End markers not found in book " + bookId);
                return false;
            }

            String[] parts = text.split(Pattern.quote(START_MARKER), 2);
            String header = parts[0].trim();
            String[] bodyParts = parts[1].split(Pattern.quote(END_MARKER), 2);
            String body = bodyParts[0].trim();

            Path bodyPath = outputDir.resolve(bookId + "_body.txt");
            Path headerPath = outputDir.resolve(bookId + "_header.txt");

            Files.writeString(bodyPath, body, StandardCharsets.UTF_8);
            Files.writeString(headerPath, header, StandardCharsets.UTF_8);

            System.out.println("[INFO] Book " + bookId + " downloaded to " + outputDir.toAbsolutePath());
            return true;

        } catch (IOException | InterruptedException e) {
            System.out.println("[ERROR] Download failed for book " + bookId + ": " + e.getMessage());
            return false;
        }
    }

    /**
     * Moves downloaded book files into the structured datalake folder.
     *
     * @param bookId       The ID of the book.
     * @param downloadPath The folder where the book was downloaded.
     * @return true if the move was successful, false otherwise.
     */
    public static boolean createDatalake(int bookId, String downloadPath) {
        try {
            String date = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
            String hour = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH"));

            Path datalakeDir = Paths.get("../datalake", date, hour);
            Files.createDirectories(datalakeDir);

            Path downloadsDir = Paths.get(downloadPath);
            Path bodySrc = downloadsDir.resolve(bookId + "_body.txt");
            Path headerSrc = downloadsDir.resolve(bookId + "_header.txt");

            if (!Files.exists(bodySrc) || !Files.exists(headerSrc)) {
                System.out.println("[ERROR] Files not found in " + downloadsDir.toAbsolutePath());
                return false;
            }

            Path bodyDst = datalakeDir.resolve(bookId + ".body.txt");
            Path headerDst = datalakeDir.resolve(bookId + ".header.txt");

            Files.move(bodySrc, bodyDst, StandardCopyOption.REPLACE_EXISTING);
            Files.move(headerSrc, headerDst, StandardCopyOption.REPLACE_EXISTING);

            System.out.println("[INFO] Files moved to datalake at " + datalakeDir.toAbsolutePath());
            return true;

        } catch (IOException e) {
            System.out.println("[ERROR] Failed to move files to datalake: " + e.getMessage());
            return false;
        }
    }
}
