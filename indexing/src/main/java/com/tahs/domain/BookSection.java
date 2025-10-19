package com.tahs.domain;

public enum BookSection {
    HEADER("header"),
    BODY("body");

    private final String fileSuffix;

    BookSection(String fileSuffix) {
        this.fileSuffix = fileSuffix;
    }

    public String fileSuffix() {
        return fileSuffix;
    }

}
