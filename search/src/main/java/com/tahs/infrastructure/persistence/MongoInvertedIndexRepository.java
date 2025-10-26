package com.tahs.infrastructure.persistence;

import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.tahs.application.ports.InvertedIndexRepository;
import com.tahs.domain.BooksTerm;
import org.bson.Document;

import java.util.List;

public class MongoInvertedIndexRepository implements InvertedIndexRepository {
    private final MongoDatabase database;
    private final MongoCollection<Document> collection;

    public MongoInvertedIndexRepository(MongoClient mongoClient, String databaseName, String collectionName) {
        this.database = mongoClient.getDatabase(databaseName);
        this.collection = this.database.getCollection(collectionName);
    }

    @Override
    public BooksTerm getBooksByTerm(String term) {
        var termDocument = this.collection.find((Filters.eq("term", term))).first();
        if(termDocument == null) {
            throw new IllegalArgumentException("Term not found");
        }

        return new BooksTerm(
                termDocument.getString("term"),
                termDocument.getList("postings", String.class)
            );
    }
}
