package com.tahs.application.ports;

public interface MetadataRepository {
    void updateMetadata(String bookId);
    void rebuildMetadata();
    void getMetadataStatus();
}
