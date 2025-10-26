package com.tahs.application.ports;

import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;

public interface DatalakeRepository {

    boolean exists(int bookId) throws IOException;

    List<Integer> listBooks() throws IOException;

    boolean saveBook(int bookId, Path stagingPath, LocalDateTime timestamp) throws IOException;

    String relativePathFor(int bookId, LocalDateTime timestamp);
}