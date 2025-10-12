package com.tahs.infrastructure.persistence;

import com.tahs.application.ports.MetadataRepository;

public class MongoMetadataRepository implements MetadataRepository {
    @Override
    public void updateMetadata(String bookId) {}
    @Override
    public void rebuildMetadata() {}
    @Override
    public void getMetadataStatus() {}
}
