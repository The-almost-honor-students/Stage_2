package com.tahs.infrastructure.persistence;

import com.tahs.application.ports.InvertedIndexRepository;

public class MongoInvertedIndexRepository implements InvertedIndexRepository {
    @Override
    public void updateIndex(String bookId) {}
    @Override
    public void rebuildIndex() {}
    @Override
    public void getIndexStatus() {}
}
