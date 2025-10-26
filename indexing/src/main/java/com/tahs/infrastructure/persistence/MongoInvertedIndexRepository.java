package com.tahs.infrastructure.persistence;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.tahs.application.ports.InvertedIndexRepository;
import com.tahs.domain.IndexStats;
import org.bson.Document;
import org.bson.types.ObjectId;
import com.mongodb.client.MongoCursor;

import java.time.Instant;
import java.util.*;

import static com.mongodb.client.model.Filters.eq;

public class MongoInvertedIndexRepository implements InvertedIndexRepository {
    private final MongoDatabase database;
    private final MongoCollection<Document> collection;
    private final Map<String, List<Integer>> index = new HashMap<>();
    private final String collectionName;
    private final String databaseName;

    public MongoInvertedIndexRepository(MongoClient mongoClient, String databaseName, String collectionName) {
        this.databaseName = databaseName;
        this.collectionName = collectionName;
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
    public void deleteAll() {
        this.collection.drop();
    }

    @Override
    public IndexStats getStats() {
        var collStats = database.runCommand(new Document("collStats", collectionName).append("scale", ScaleToMB()));
        double storageMB = toDouble(collStats.get("storageSize"));
        double indexesMB = toDouble(collStats.get("totalIndexSize"));
        double sizeMB = storageMB + indexesMB;
        MongoCollection<Document> coll = database.getCollection(collectionName);
        Instant lastUpdate = getLastUpdateFromUpdatedAt(coll);
        if (lastUpdate == null) {
            lastUpdate = getLastInsertFromObjectId(coll);
        }
        return new IndexStats(sizeMB, lastUpdate);

    }
    private static double toDouble(Object number) {
        return number instanceof Number ? ((Number) number).doubleValue() : 0.0;
    }

    private static int ScaleToMB() {
        return 1024 * 1024;
    }

    private static Instant getLastUpdateFromUpdatedAt(MongoCollection<Document> coll) {
        try (MongoCursor<Document> cursor = coll.aggregate(Arrays.asList(
                new Document("$group", new Document("_id", null)
                        .append("last", new Document("$max", "$updated_at")))
        )).iterator()) {
            if (cursor.hasNext()) {
                Date d = cursor.next().getDate("last");
                return d != null ? d.toInstant() : null;
            }
        }
        return null;
    }

    private static Instant getLastInsertFromObjectId(MongoCollection<Document> coll) {
        Document newest = coll.find()
                .sort(Sorts.descending("_id"))
                .limit(1)
                .first();
        if (newest != null) {
            ObjectId id = newest.getObjectId("_id");
            if (id != null) {
                return Instant.ofEpochSecond(id.getTimestamp());
            }
        }
        return null;
    }


}
