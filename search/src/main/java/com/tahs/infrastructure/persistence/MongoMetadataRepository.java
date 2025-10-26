package com.tahs.infrastructure.persistence;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.tahs.application.ports.MetadataRepository;
import com.tahs.domain.BookMetadata;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class MongoMetadataRepository implements MetadataRepository {

    private final MongoDatabase database;
    private final MongoCollection<Document> collection;

    public MongoMetadataRepository(MongoClient mongoClient, String databaseName, String collectionName) {
        this.database = mongoClient.getDatabase(databaseName);
        this.collection = this.database.getCollection(collectionName);
    }

    @Override
    public BookMetadata getById(String bookId) {
        Document bookDocument =  this.collection.find((Filters.eq("book_id", Integer.parseInt(bookId)))).first();
        if(bookDocument == null) {
            throw new IllegalArgumentException("Term not found");
        }
        return new BookMetadata(
                bookDocument.getInteger("book_id"),
                bookDocument.getString("title"),
                bookDocument.getString("author"),
                bookDocument.getString("language"));
    }
}
