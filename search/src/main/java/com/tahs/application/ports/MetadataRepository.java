package com.tahs.application.ports;

import com.tahs.domain.BookMetadata;

import java.util.List;

public interface MetadataRepository {
    BookMetadata getById(String bookId);
}
