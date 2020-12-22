package com.company;

import com.company.entities.User;
import com.mongodb.*;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.*;
import com.mongodb.client.internal.MongoClientImpl;
import com.mongodb.client.model.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Main {

    public static void main(String[] args) {

        System.out.println("-------------USERS------------");
        Main.printCollection("users");

        System.out.println("-------------ACCOUNTS------------");
        Main.printCollection("accounts");

        System.out.println("\n\n-------------CARDS------------");
        Main.printCollection("cards");



        MongoClient mongoClient = MongoClients.create("mongodb://127.0.0.1:27017");
        MongoDatabase database = mongoClient.getDatabase("bank");
        MongoCollection<Document> collection =  database.getCollection("users");

        Document doc =  collection.find(new Document().append("name", "Dmytro").append("surname", "Bevza")).first();
        System.out.println(doc.get("surname"));


        MongoCollection<Document> coll = database.getCollection("users");
        Document unwind1 = new Document("$unwind", new Document("path", "$cards"));
//        coll.aggregate(Arrays.asList(unwind1)).forEach(result -> System.out.println(result.toJson()));

        coll = database.getCollection("accounts");
        Document addFields = new Document("$addFields", new Document("currency", "$balance.currency"));
//        coll.aggregate(Arrays.asList(addFields)).forEach(result -> System.out.println(result.toJson()));

        coll = database.getCollection("users");
        Document lookup = new Document("$lookup", new Document("from", "cards").append("localField",
                "cards").append("foreignField", "_id").append("as", "cardsData"));
//        coll.aggregate(Arrays.asList(unwind1, lookup)).forEach(result -> System.out.println(result.toJson()));

        Document sort = new Document("$sort", new Document("cardsCount", 1));
        Document group = new Document("$group", new Document("_id", "$name").append("cardsCount", new Document(
                "$sum", 1
        )));
        coll.aggregate(Arrays.asList(unwind1, lookup, group, sort)).forEach(result -> System.out.println(result.toJson()));
    }

    private static void printCollection(String name) {
        MongoClient mongoClient = MongoClients.create("mongodb://127.0.0.1:27017");
        MongoDatabase database = mongoClient.getDatabase("bank");
        MongoCollection<Document> collection =  database.getCollection(name);

        collection.find().forEach(doc -> {
            System.out.println("_______");
            for (String key: doc.keySet()
                    ) {
                System.out.println(key +": " +doc.get(key));
            }
        });

    }


}
