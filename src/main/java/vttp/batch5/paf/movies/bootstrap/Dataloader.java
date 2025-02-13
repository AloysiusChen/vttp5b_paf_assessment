package vttp.batch5.paf.movies.bootstrap;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Date;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import jakarta.json.Json;
import jakarta.json.JsonReader;
import jakarta.json.stream.JsonParsingException;
import jakarta.json.JsonObject;
import vttp.batch5.paf.movies.models.MovieSQL;
import vttp.batch5.paf.movies.repositories.MongoMovieRepository;
import vttp.batch5.paf.movies.repositories.MySQLMovieRepository;

import java.io.StringReader;
import static vttp.batch5.paf.movies.repositories.Constants.*;

@Component
public class Dataloader implements CommandLineRunner {
  @Autowired
  private MongoMovieRepository mongoMovieRepo;

  @Autowired
  private MySQLMovieRepository mySQLMovieRepo;

  // TODO: Task 2
  private final Logger logger = Logger.getLogger(Dataloader.class.getName());

  @Override
  public void run(String... args) throws Exception {

    // Task 2.1: determine if the contents of movies_post_2010.zip have been loaded
    if (mongoMovieRepo.isMongoDbPopulated()) {
      logger.info("Database is already populated. Skipping data loading.");
      return;
    }

    if (args.length == 0) {
      logger.warning("No file path provided");
      return;
    }

    String[] terms = args[0].split("=");
    if (terms.length != 2) {
      logger.warning("Invalid argument format. Expected: file=path");
      return;
    }

    Path p = Paths.get(terms[1]);
    logger.info("Processing file: " + p.toString());

    logger.info("Opening ZIP file...");

    try (FileInputStream fis = new FileInputStream(p.toFile());
        ZipInputStream zis = new ZipInputStream(fis)) {

      ZipEntry zipEntry = zis.getNextEntry();
      logger.info("Found entry in ZIP: " + zipEntry.getName());

      BufferedReader br = new BufferedReader(new InputStreamReader(zis));
      String line;
      while ((line = br.readLine()) != null) {
        if (!line.trim().isEmpty()) {
          try {
            try (JsonReader jsonReader = Json.createReader(new StringReader(line))) {
              JsonObject jsonObject = jsonReader.readObject();
              Document doc = Document.parse(jsonObject.toString());
              logger.info("jsonObject is now a Document");

              String releaseDate = doc.getString("release_date");
              logger.info("releaseDate obtained");

              int year = Integer.parseInt(releaseDate.substring(0, 4));
              logger.info("year obtained");
              logger.info("%s".formatted(year));
              if (year >= 2018) {
                Document cleanedDoc = imputeMissingValues(doc);
                logger.info("document cleaned");

                Document selectedDoc = new Document();
                selectedDoc.put("_id", cleanedDoc.get("imdb_id"));
                selectedDoc.put("title", cleanedDoc.get("title"));
                selectedDoc.put("director", cleanedDoc.get("director"));
                selectedDoc.put("overview", cleanedDoc.get("overview"));
                selectedDoc.put("tagline", cleanedDoc.get("tagline"));
                selectedDoc.put("genres", cleanedDoc.get("genres"));
                selectedDoc.put("imdb_rating", cleanedDoc.get("imdb_rating"));
                selectedDoc.put("imdb_votes", cleanedDoc.get("imdb_votes"));

                MovieSQL movieSQL = new MovieSQL();
                movieSQL.setImdb_id((String)cleanedDoc.get("imdb_id"));
                movieSQL.setVote_average((Float)cleanedDoc.get("vote_average"));
                movieSQL.setVote_count((Integer)cleanedDoc.get("vote_count"));
                movieSQL.setRelease_date((Date) cleanedDoc.get("release_date"));
                movieSQL.setRevenue((Double) cleanedDoc.get("revenue"));
                movieSQL.setBudget((Double) cleanedDoc.get("budget"));
                movieSQL.setRuntime((Integer) cleanedDoc.get("runtime"));

                boolean added = mySQLMovieRepo.insertMovie(movieSQL);
                mongoMovieRepo.batchInsertMovies(selectedDoc, MONGODB_C_MOVIES);
              }
            } catch (JsonParsingException e) {
              logger.warning("Error parsing JSON line: " + e.getMessage());
              logger.warning("Problematic line: " + line);
            }
          } catch (Exception e) {
            logger.warning("Error processing line: " + e.getMessage());
          }
        }
      }
      zis.closeEntry();
    }
  }

  private Document imputeMissingValues(Document doc) {
    Document cleanedDoc = new Document();
    cleanedDoc.put("_id", doc.getObjectId("_id"));
    cleanedDoc.put("title", doc.get("title", ""));
    cleanedDoc.put("vote_average", doc.get("vote_average", 0));
    cleanedDoc.put("vote_count", doc.get("vote_count", 0));
    cleanedDoc.put("status", doc.get("status", ""));
    cleanedDoc.put("release_date", doc.get("release_date", ""));
    cleanedDoc.put("revenue", doc.get("revenue", 0));
    cleanedDoc.put("runtime", doc.get("runtime", 0));
    cleanedDoc.put("budget", doc.get("budget", 0));
    cleanedDoc.put("imdb_id", doc.get("imdb_id", ""));
    cleanedDoc.put("original_language", doc.get("original_language", ""));
    cleanedDoc.put("overview", doc.get("overview", ""));
    cleanedDoc.put("popularity", doc.get("popularity", 0));
    cleanedDoc.put("tagline", doc.get("tagline", ""));
    cleanedDoc.put("genres", doc.get("genres", ""));
    cleanedDoc.put("spoken_languages", doc.get("spoken_languages", ""));
    cleanedDoc.put("casts", doc.get("casts", ""));
    cleanedDoc.put("director", doc.get("director", ""));
    cleanedDoc.put("imdb_rating", doc.get("imdb_rating", 0));
    cleanedDoc.put("imdb_votes", doc.get("imdb_votes", 0));
    cleanedDoc.put("poster_path", doc.get("poster_path", ""));

    return cleanedDoc;
  }
}