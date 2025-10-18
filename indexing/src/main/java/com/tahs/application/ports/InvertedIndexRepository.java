package com.tahs.application.ports;

import com.tahs.domain.Book;
import java.util.List;
import java.util.Map;

public interface InvertedIndexRepository {

    boolean indexBook(String book_id);

    List<Integer> getIndexByTerm(String term);

    Map<String, Integer> getIndexStats();
}
