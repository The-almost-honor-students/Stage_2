package com.tahs.application.ports;

public class SearchHit {
    private String bookId;
    private double score;

    public SearchHit(String bookId, double score) {
        this.bookId = bookId;
        this.score = score;
    }

    public String getBookId() {
        return bookId;
    }

    public double getScore() {
        return score;
    }
}

