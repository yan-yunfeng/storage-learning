package curd;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;

import org.bson.Document;

import connector.Connectors;

/**
 * author Yan YunFeng  Email:twd.wuyun@163.com
 * create 19-8-9 下午3:01
 */
public class MongoDB {

    public static void main(String[] args) {

        MongoClient mongoClient = Connectors.getMongoClient();

        MongoCollection<Document> mongoCollection = mongoClient.getDatabase("database")
                                                               .getCollection("collection");

        //查
        mongoCollection.find(new Document("key","value"))
                       .iterator()
                       .forEachRemaining(document -> {
                           System.out.println(document.toJson());
                       });

        //增
        mongoCollection.insertOne(new Document("key","value"));

        //改
        mongoCollection.updateOne(new Document("key","value"),new Document("$set",new Document()));

        //删
        mongoCollection.deleteOne(new Document("key","value"));
    }
}
