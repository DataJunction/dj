package io.datajunction.models.requests;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class Catalog {
    private String name;

    public Catalog(@JsonProperty("name") String name) {
        this.name = name;
    }
}