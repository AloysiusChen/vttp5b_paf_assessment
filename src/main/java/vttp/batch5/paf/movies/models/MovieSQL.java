package vttp.batch5.paf.movies.models;

import java.sql.Date;

public class MovieSQL {
    private String imdb_id;
    private Float vote_average;
    private Integer vote_count;
    private Date release_date;
    private Double revenue;
    private Double budget;
    private Integer runtime;

    public String getImdb_id() {
        return imdb_id;
    }

    public void setImdb_id(String imdb_id) {
        this.imdb_id = imdb_id;
    }

    public Float getVote_average() {
        return vote_average;
    }

    public void setVote_average(Float vote_average) {
        this.vote_average = vote_average;
    }

    public Integer getVote_count() {
        return vote_count;
    }

    public void setVote_count(Integer vote_count) {
        this.vote_count = vote_count;
    }

    public Date getRelease_date() {
        return release_date;
    }

    public void setRelease_date(Date release_date) {
        this.release_date = release_date;
    }

    public Double getRevenue() {
        return revenue;
    }

    public void setRevenue(Double revenue) {
        this.revenue = revenue;
    }

    public Double getBudget() {
        return budget;
    }

    public void setBudget(Double budget) {
        this.budget = budget;
    }

    public Integer getRuntime() {
        return runtime;
    }

    public void setRuntime(Integer runtime) {
        this.runtime = runtime;
    }

    public MovieSQL() {
    }

    public MovieSQL(String imdb_id, Float vote_average, Integer vote_count, Date release_date, Double revenue,
            Double budget, Integer runtime) {
        this.imdb_id = imdb_id;
        this.vote_average = vote_average;
        this.vote_count = vote_count;
        this.release_date = release_date;
        this.revenue = revenue;
        this.budget = budget;
        this.runtime = runtime;
    }

}
