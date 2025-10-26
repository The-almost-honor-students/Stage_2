package com.tahs.application.ports;

public class BookMetadata {
    private String bookId;
    private String title;
    private String author;
    private int year;


    public BookMetadata(String bookId, String title, String author, int year) {
        this.bookId = bookId;
        this.title = title;
        this.author = author;
        this.year = year;
    }

    public String getBookId() {
        return bookId;
    }

    public String getTitle() { return title; }
    public String getAuthor() { return author; }
    public int getYear() { return year; }
}