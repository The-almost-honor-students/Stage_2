package com.tahs.domain;

import java.util.HashMap;
import java.util.Map;

public class Book {

    private Integer bookId;
    private String title;
    private String author;
    private String language;

    public Book(Integer bookId, String title, String author, String language) {
        this.bookId = bookId;
        this.title = title;
        this.author = author;
        this.language = language;
    }

    public Integer getBookId() {
        return bookId;
    }

    public String getTitle() {
        return title;
    }

    public String getAuthor() {
        return author;
    }

    public String getLanguage() {
        return language;
    }

    public Map<String, Object> toDict() {
        Map<String, Object> map = new HashMap<>();
        map.put("book_id", bookId);
        map.put("title", title);
        map.put("author", author);
        map.put("language", language);
        return map;
    }
}