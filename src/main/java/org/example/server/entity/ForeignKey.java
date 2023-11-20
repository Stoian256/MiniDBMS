package org.example.server.entity;

import java.util.*;
public class ForeignKey {
    String refTableName;
    List<String> attributes;
    List<String> refAttributeList;

    public ForeignKey(String refTableName, List<String> attributes, List<String> refAttributeList) {
        this.refTableName = refTableName;
        this.attributes = attributes;
        this.refAttributeList = refAttributeList;
    }

    public ForeignKey(String refTableName, List<String> attributes) {
        this.refTableName = refTableName;
        this.attributes = attributes;
    }

    public List<String> getRefAttributeList() {
        return refAttributeList;
    }

    public String getRefTableName() {
        return refTableName;
    }

    public List<String> getAttributes() {
        return attributes;
    }
}
