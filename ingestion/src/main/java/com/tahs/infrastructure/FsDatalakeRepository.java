package com.tahs.infrastructure;

import com.tahs.application.ports.DatalakeRepository;

import java.io.IOException;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;

public class FsDatalakeRepository implements DatalakeRepository {

    private final Path datalakeRoot;

    public FsDatalakeRepository(String datalakeRoot) {
        this.datalakeRoot = Paths.get(datalakeRoot).toAbsolutePath().normalize();
    }

    @Override
    public boolean exists(int bookId) throws IOException {
        if (!Files.exists(datalakeRoot)) return false;
        try (var stream = Files.walk(datalakeRoot)) {
            return stream
                    .filter(Files::isRegularFile)
                    .anyMatch(p -> p.getFileName().toString().startsWith(bookId + ".body.txt"));
        }
    }

    @Override
    public List<Integer> listBooks() throws IOException {
        if (!Files.exists(datalakeRoot)) return List.of();
        try (var stream = Files.walk(datalakeRoot)) {
            return stream
                    .filter(Files::isRegularFile)
                    .map(p -> p.getFileName().toString())
                    .filter(name -> name.endsWith(".body.txt"))
                    .map(name -> name.split("\\.")[0])
                    .map(Integer::parseInt)
                    .distinct()
                    .sorted()
                    .collect(Collectors.toList());
        }
    }

    @Override
    public boolean saveBook(int bookId, Path stagingPath, LocalDateTime timestamp) throws IOException {
        String date = timestamp.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String hour = timestamp.format(DateTimeFormatter.ofPattern("HH"));

        Path targetDir = datalakeRoot.resolve(date).resolve(hour);
        Files.createDirectories(targetDir);

        Path bodySrc = stagingPath.resolve(bookId + "_body.txt");
        Path headerSrc = stagingPath.resolve(bookId + "_header.txt");

        if (!Files.exists(bodySrc) || !Files.exists(headerSrc)) {
            throw new IOException("Missing source files for book " + bookId);
        }

        Path bodyDst = targetDir.resolve(bookId + ".body.txt");
        Path headerDst = targetDir.resolve(bookId + ".header.txt");

        Files.move(bodySrc, bodyDst, StandardCopyOption.REPLACE_EXISTING);
        Files.move(headerSrc, headerDst, StandardCopyOption.REPLACE_EXISTING);

        System.out.println("[INFO] Book " + bookId + " moved to datalake at " + targetDir);
        return true;
    }

    @Override
    public String relativePathFor(int bookId, LocalDateTime timestamp) {
        String date = timestamp.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String hour = timestamp.format(DateTimeFormatter.ofPattern("HH"));
        return String.format("datalake/%s/%s/%d", date, hour, bookId);
    }
}
