package com.tahs.domain;

import com.tahs.application.ports.BookMetadata;

public class RankedBook {
    private final BookMetadata metadata;
    private final double score;

    public RankedBook(BookMetadata metadata, double score) {
        this.metadata = metadata;
        this.score = score;
    }

    public BookMetadata getMetadata() {
        return metadata;
    }

    public double getScore() {
        return score;
    }
}
