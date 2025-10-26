package com.tahs.application.dto;

import java.time.Instant;

public record StatsDto(int books_indexed, double sizeMB, Instant lastUpdate) {}

