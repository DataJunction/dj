package io.datajunction.client;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URI;
import java.io.IOException;

public class DJClient {

    HttpClient client = HttpClient.newHttpClient();
    String baseURL;
    String namespace;
    String engineName;
    String engineVersion;


    public DJClient(HttpClient client,String baseURL, String namespace, String engineName,String engineVersion){
        this.client = client;
        this.baseURL = baseURL;
        this.namespace = namespace;
        this.engineName = engineName;
        this.engineVersion = engineVersion;
    }

    public DJClient(String baseURL, String namespace, String engineName,String engineVersion){
        this.baseURL = baseURL;
        this.namespace = namespace;
        this.engineName = engineName;
        this.engineVersion = engineVersion;
    }

    public DJClient () {
        this.baseURL = "http://localhost:8000";
        this.namespace = "default";
        this.engineName = null;
        this.engineVersion = null;
    }

    public HttpResponse getHelper(String endpoint) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .GET()
                .uri(URI.create(baseURL+endpoint))
                .setHeader("User-Agent", "Java 11 HttpClient Bot") // add request header
                .build();

        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    public String listMetrics() throws IOException, InterruptedException {
        HttpResponse<String> response = getHelper("/metrics/");
        return response.body();
    }




}
