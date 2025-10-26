package com.tahs.tracker;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.*;

public class DownloadTracker {

    private static final Path FILE_PATH = Paths.get("control", "downloaded_books.txt");

    public static void createFileIfNotExists() throws IOException {
        try {
            if (Files.notExists(FILE_PATH)) {
                Files.createFile(FILE_PATH);
            }
        } catch (IOException e) {
            throw new RuntimeException("Cannot create downloaded_books.txt", e);
        }
    }

    public static void markAsDownloaded(String bookId) {
        try (FileWriter writer = new FileWriter(FILE_PATH.toFile(), true)) {
            writer.write(bookId + System.lineSeparator());
        } catch (IOException e) {
            System.err.println("Error writing to downloaded_books.txt: " + e.getMessage());
        }
    }

    public static boolean isDownloaded(String bookId) {
        try {
            return Files.lines(FILE_PATH)
                    .anyMatch(line -> line.trim().equals(bookId));
        } catch (IOException e) {
            System.err.println("Error reading downloaded_books.txt: " + e.getMessage());
            return false;
        }
    }

    public static int countDownloadedBooks() {
        try {
            return (int) Files.lines(FILE_PATH).count();
        } catch (IOException e) {
            System.err.println("Error reading downloaded_books.txt: " + e.getMessage());
            return 0;
        }
    }
}
