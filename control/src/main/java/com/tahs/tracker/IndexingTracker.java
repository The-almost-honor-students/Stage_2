package com.tahs.tracker;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class IndexingTracker {
    private static final Path DATA_DIR = Paths.get("control");
    private static final Path FILE_PATH = Paths.get("indexed_books.txt");

    public static void createFileIfNotExists() throws IOException {
        try {
            Files.createDirectories(DATA_DIR);
            if (Files.notExists(FILE_PATH)) {
                Files.createFile(FILE_PATH);
            }
        } catch (IOException e) {
            throw new RuntimeException("Cannot create indexed_books.txt", e);
        };
    }


    public static void markAsIndexed(String bookId) {
        try (FileWriter writer = new FileWriter(FILE_PATH.toFile(), true)) {
            writer.write(bookId + System.lineSeparator());
        } catch (IOException e) {
            System.err.println("Error writing to indexed_books.txt: " + e.getMessage());
        }
    }

    public static boolean isIndexed(String bookId) {
        try {
            return Files.lines(FILE_PATH)
                    .anyMatch(line -> line.trim().equals(bookId));
        } catch (IOException e) {
            System.err.println("Error reading indexed_books.txt: " + e.getMessage());
            return false;
        }
    }

    public static int countIndexedBooks() {
        try {
            return (int) Files.lines(FILE_PATH).count();
        } catch (IOException e) {
            System.err.println("Error reading indexed_books.txt: " + e.getMessage());
            return 0;
        }
    }
}
