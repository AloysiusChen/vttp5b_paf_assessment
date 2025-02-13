package vttp.batch5.paf.movies.repositories;

import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import vttp.batch5.paf.movies.models.MovieSQL;

public class MySQLMovieRepository {
  @Autowired
  private JdbcTemplate template;

  // TODO: Task 2.3
  // You can add any number of parameters and return any type from the method
  public int[] batchInsertMovies(List<MovieSQL> movies) {
    List<Object[]> params = movies.stream()
        .map(movie -> new Object[] { movie.getImdb_id(), movie.getVote_average(),
            movie.getVote_count(), movie.getRelease_date(), movie.getRevenue(), movie.getBudget(),
            movie.getRuntime() })
        .collect(Collectors.toList());

    int added[] = template.batchUpdate(
        "INSERT INTO movies(imdb_id, vote_average, vote_count, release_date, revenue, budget, runtime) VALUES (?, ?, ?, ?, ?, ?, ?)",
        params);

    return added;
  }

  public boolean insertMovie(MovieSQL movie) {
    int added = template.update(
        "INSERT INTO movies(imdb_id, vote_average, vote_count, release_date, revenue, budget, runtime) VALUES (?, ?, ?, ?, ?, ?, ?)",
        movie.getImdb_id(),
        movie.getVote_average(),
        movie.getVote_count(),
        movie.getRelease_date(),
        movie.getRevenue(),
        movie.getBudget(),
        movie.getRuntime());

    return added > 0;
  }

  // TODO: Task 3
  
}
