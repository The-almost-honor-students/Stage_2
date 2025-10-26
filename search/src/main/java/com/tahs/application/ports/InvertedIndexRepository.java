package com.tahs.application.ports;

import com.tahs.domain.BooksTerm;

import java.util.List;

public interface InvertedIndexRepository {
    BooksTerm getBooksByTerm(String term);
}
