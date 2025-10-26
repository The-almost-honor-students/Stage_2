package com.tahs.domain;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        Book book = (Book) o;
        return Objects.equals(bookId, book.bookId) && Objects.equals(title, book.title) && Objects.equals(author, book.author) && Objects.equals(language, book.language);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bookId, title, author, language);
    }
}