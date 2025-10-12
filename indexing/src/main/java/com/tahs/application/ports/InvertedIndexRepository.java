package com.tahs.application.ports;

public interface InvertedIndexRepository {
    void updateIndex(String bookId);
    void rebuildIndex();
    void getIndexStatus();
}
