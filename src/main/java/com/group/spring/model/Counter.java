package com.group.spring.model;

public class Counter {
    private String Property;
    private Long Count;

    public Counter() {
    }

    public Counter(String property, Long count) {
        Property = property;
        Count = count;
    }

    public String getProperty() {
        return Property;
    }

    public void setProperty(String property) {
        Property = property;
    }

    public Long getCount() {
        return Count;
    }

    public void setCount(Long count) {
        Count = count;
    }
}
