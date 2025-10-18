package com.tahs.infrastructure.persistence;

import com.tahs.application.ports.InvertedIndexRepository;
import com.tahs.domain.Book;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MongoInvertedIndexRepository implements InvertedIndexRepository {

    private final Map<String, List<Integer>> index = new HashMap<>();

    @Override
    public boolean indexBook(String bookId) {
        return false;
    }

    @Override
    public List<Integer> getIndexByTerm(String term) {
        return index.getOrDefault(term.toLowerCase(), new ArrayList<>());
    }

    @Override
    public Map<String, Integer> getIndexStats() {
        Map<String, Integer> stats = new HashMap<>();
        stats.put("terms", index.size());
        stats.put("books_indexed", index.values().stream().mapToInt(List::size).sum());
        return stats;
    }
}
