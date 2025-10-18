package com.tahs.infrastructure.persistence;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.tahs.application.ports.MetadataRepository;
import com.tahs.domain.Book;
import org.bson.Document;

import java.util.Map;

public class MongoMetadataRepository implements MetadataRepository {

    private final MongoDatabase database;
    private final MongoCollection<Document> collection;

    public MongoMetadataRepository(MongoClient mongoClient, String databaseName, String collectionName) {
        this.database = mongoClient.getDatabase(databaseName);
        this.collection = this.database.getCollection(collectionName);
    }

    @Override
    public void save(Book book) {
        Map<String, Object> map = book.toDict();
        Document doc = new Document(map);
        this.collection.insertOne(doc);
         System.out.println("Book " + book.getBookId() + " saved in MongoDB");
    }
    @Override
    public void rebuildMetadata() {}
    @Override
    public void getMetadataStatus() {}
}
