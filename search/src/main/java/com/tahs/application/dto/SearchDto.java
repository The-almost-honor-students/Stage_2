package com.tahs.application.dto;

import com.tahs.domain.BookMetadata;

import java.util.List;

public record SearchDto (
        String query,
        java.util.Map<String, List<String>> filters,
        int count,
        List<BookMetadata> books
){}
