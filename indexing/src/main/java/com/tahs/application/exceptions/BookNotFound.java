package com.tahs.application.exceptions;

public class BookNotFound extends RuntimeException {
    public BookNotFound(String bookId) {
        super("Not found book with ID: " + bookId);
    }
}
