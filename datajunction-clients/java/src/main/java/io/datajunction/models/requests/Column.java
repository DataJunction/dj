package io.datajunction.models.requests;

import lombok.Data;

@Data
public class Column {
    private String name;
    private String type;

    public Column(String name, String type) {
        this.name = name;
        this.type = type;
    }
}