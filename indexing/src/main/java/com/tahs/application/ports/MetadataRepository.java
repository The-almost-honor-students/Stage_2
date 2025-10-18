package com.tahs.application.ports;

import com.tahs.domain.Book;

public interface MetadataRepository {
    void save(Book book);
    void rebuildMetadata();
    void getMetadataStatus();
}
