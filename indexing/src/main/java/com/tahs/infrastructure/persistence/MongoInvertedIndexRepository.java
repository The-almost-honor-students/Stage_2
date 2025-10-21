package com.tahs.infrastructure.persistence;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.tahs.application.ports.InvertedIndexRepository;
import org.bson.Document;

import java.util.*;

import static com.mongodb.client.model.Filters.eq;

public class MongoInvertedIndexRepository implements InvertedIndexRepository {
    private final MongoDatabase database;
    private final MongoCollection<Document> collection;
    private final Map<String, List<Integer>> index = new HashMap<>();

    public MongoInvertedIndexRepository(MongoClient mongoClient, String databaseName, String collectionName) {
        this.database = mongoClient.getDatabase(databaseName);
        this.collection = this.database.getCollection(collectionName);
    }

    @Override
    public boolean indexBook(String bookId, Set<String> terms) {
        for( String term : terms){
            var filter = eq("term", term);
            var update = Updates.combine(
                    Updates.setOnInsert("term", term),
                    Updates.addToSet("postings", bookId)
            );
            collection.updateOne(filter, update, new UpdateOptions().upsert(true));
        }
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
