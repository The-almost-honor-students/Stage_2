package com.tahs.application.usecase;

import com.tahs.application.ports.InvertedIndexRepository;
import com.tahs.application.ports.MetadataRepository;

public class IndexService {

    private final MetadataRepository metadataRepository;
    private final InvertedIndexRepository indexRepository;

    public IndexService(InvertedIndexRepository indexRepository, MetadataRepository metadataRepository) {
        this.indexRepository = indexRepository;
        this.metadataRepository = metadataRepository;
    }

    public void updateByBookId(String bookId) {
    }
}
