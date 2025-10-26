package com.tahs.domain;

import java.util.List;

public record BooksTerm (
    String term,
    List<String> booksId
){}
