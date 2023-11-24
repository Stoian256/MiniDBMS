package org.example.server.entity;

import java.util.*;
public class IndexFile {
    private String indexFileName;
    private List<String> attributes;

    public IndexFile(String indexFileName, List<String> attributes) {
        this.indexFileName = indexFileName;
        this.attributes = attributes;
    }

    public String getIndexFileName() {
        return indexFileName;
    }

    public List<String> getAttributes() {
        return attributes;
    }
}
