package com.tahs.infrastructure.serialization.books;

import com.tahs.domain.Book;
import com.tahs.infrastructure.serialization.books.GutenbergHeaderSerializer;
import org.junit.jupiter.api.*;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

public class GutenbergHeaderSerializerTest {

    @Test
    void read_book_header_datalake() throws IOException {
        var givenABookHeaderPath = "src/main/test/datalake/20251018/18/6036.header.txt";

        var gunteber_serializer = new GutenbergHeaderSerializer();
        Book book = gunteber_serializer.deserialize(givenABookHeaderPath);

        Book expectedBook = new Book(6036, "The Kasîdah of Hâjî Abdû El-Yezdî", "Sir Richard Francis Burton", "English");
        assertEquals(expectedBook,book);
    }
}
