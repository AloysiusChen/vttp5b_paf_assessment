package vttp.batch5.paf.movies.repositories;

import java.util.List;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.GroupOperation;
import org.springframework.data.mongodb.core.aggregation.SortOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

import static vttp.batch5.paf.movies.repositories.Constants.*;

@Repository
public class MongoMovieRepository {

   @Autowired
   private MongoTemplate template;

   // TODO: Task 2.3
   // You can add any number of parameters and return any type from the method
   // You can throw any checked exceptions from the method
   // Write the native Mongo query you implement in the method in the comments
   //
   // native MongoDB query here
   //
   /*
    * db.movies.insertMany([
    * {_id: 'abc', ...},
    * {_id: 'def', ...},
    * ])
    */
   public <T> T batchInsertMovies(T doc, String colName) {
      return template.insert(doc, colName);
   }

   // TODO: Task 2.4
   // You can add any number of parameters and return any type from the method
   // You can throw any checked exceptions from the method
   // Write the native Mongo query you implement in the method in the comments
   //
   // native MongoDB query here
   //
   public void logError() {

   }

   // TODO: Task 3
   // Write the native Mongo query you implement in the method in the comments
   //
   // native MongoDB query here
   //
   /*
    * db.movies.aggregate([
    * {
    * $group: {
    * _id: "$director",
    * noOfMovies: { $sum: 1 }
    * }
    * },
    * {
    * $sort: { noOfMovies: -1 }
    * }
    * ])
    */
   public List<Document> getDirectorStats() {
      GroupOperation groupByDirector = Aggregation.group("director")
            .count().as("noOfMovies");

      SortOperation sortByCount = Aggregation.sort(Sort.Direction.DESC, "noOfMovies");

      Aggregation pipeline = Aggregation.newAggregation(groupByDirector, sortByCount);

      AggregationResults<Document> results = template.aggregate(
            pipeline, MONGODB_C_MOVIES, Document.class);

      return results.getMappedResults();
   }

   // Check if MongoDB is populated based on existence of 'imdb_id' field
   public Boolean isMongoDbPopulated() {
      Criteria criteria = Criteria.where("imdb_id").exists(true);
      Query query = Query.query(criteria);

      List<Document> results = template.find(query, Document.class, MONGODB_C_MOVIES);

      if (results.isEmpty()) {
         return false;
      } else {
         return true;
      }
   }

}
