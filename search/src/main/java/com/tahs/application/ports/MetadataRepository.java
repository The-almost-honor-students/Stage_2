package com.tahs.application.ports;

import java.util.List;

public interface MetadataRepository {
    BookMetadata findById(String bookId);
    List<BookMetadata> findByIds(List<String> bookIds);
}