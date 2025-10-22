package com.tahs.infrastructure.persistence;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.tahs.application.ports.MetadataRepository;
import com.tahs.domain.Book;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
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
    public void deleteAll() {
        this.collection.drop();
    }

    @Override
    public List<Book> getAll() {
        List<Book> books = new ArrayList<>();
        for (Document doc : this.collection.find()) {
            var book = new Book(
                    doc.getInteger("book_id"),
                    doc.getString("title"),
                    doc.getString("author"),
                    doc.getString("language")
            );
            books.add(book);
        }
        return books;
    }


}
