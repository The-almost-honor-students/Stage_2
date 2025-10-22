package com.tahs.application.ports;

import com.tahs.domain.Book;

import java.util.List;

public interface MetadataRepository {
    void save(Book book);

    void deleteAll();

    List<Book> getAll();
}
