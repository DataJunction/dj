package io.datajunction.models.requests;

import lombok.Data;

@Data
public class Engine {
    private String name;
    private String version;

    public Engine(String name, String version) {
        this.name = name;
        this.version = version;
    }
}