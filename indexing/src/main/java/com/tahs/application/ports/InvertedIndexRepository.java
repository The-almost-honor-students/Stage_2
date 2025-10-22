package com.tahs.application.ports;

import com.tahs.domain.IndexStats;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface InvertedIndexRepository {

    boolean indexBook(String book_id, Set<String> terms);
    void deleteAll();

    IndexStats getStats();
}
