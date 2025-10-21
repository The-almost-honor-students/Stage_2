package com.tahs.application.ports;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface InvertedIndexRepository {

    boolean indexBook(String book_id, Set<String> terms);

    List<Integer> getIndexByTerm(String term);

    Map<String, Integer> getIndexStats();
}
