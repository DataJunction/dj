package io.datajunction.models.requests;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class Catalog {
    private String name;

    public Catalog(String name) {
        this.name = name;
    }
}