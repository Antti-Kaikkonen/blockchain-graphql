package io.github.anttikaikkonen.bitcoinrpcclientjava;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.anttikaikkonen.bitcoinrpcclientjava.models.Block;
import io.github.anttikaikkonen.bitcoinrpcclientjava.models.BlockHeader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.util.EntityUtils;

public class RpcClientImpl implements RpcClient {
    
    private final ObjectMapper objectMapper;
    private final CloseableHttpAsyncClient client;
    private final String url;
    
    private static final ContentType JSON = ContentType.APPLICATION_JSON.withCharset(StandardCharsets.UTF_8);
    
    public RpcClientImpl(CloseableHttpAsyncClient client, String url) {
        this.client = client;
        this.url = url;
        objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public CompletionStage<Integer> getBlockCount() {
        try {
            HttpPost post = new HttpPost(url);
            ObjectNode body = objectMapper.createObjectNode().put("method", "getblockcount");
            post.setEntity(new StringEntity(objectMapper.writeValueAsString(body), JSON));
            CompletableFuture<Integer> resultFuture = new CompletableFuture<>();
            this.client.execute(post, new FutureCallback<HttpResponse>() {
                @Override
                public void completed(HttpResponse response) {
                    try (InputStream content = response.getEntity().getContent()) {
                        int res = objectMapper.readTree(EntityUtils.toString(response.getEntity())).get("result").asInt();
                        resultFuture.complete(res);
                    } catch (Exception ex) { 
                        resultFuture.completeExceptionally(ex);
                    }
                }

                @Override
                public void failed(Exception excptn) {
                    resultFuture.completeExceptionally(excptn);
                }

                @Override
                public void cancelled() {
                    resultFuture.cancel(true);
                }
            });
            return (CompletionStage) resultFuture;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public CompletionStage<String> getBlockHash(int height) {
        
        try {
            HttpPost post = new HttpPost(url);
            ObjectNode body = objectMapper.createObjectNode().put("method", "getblockhash");
            body.putArray("params").add(height);
            post.setEntity(new StringEntity(objectMapper.writeValueAsString(body), JSON));
            CompletableFuture<String> resultFuture = new CompletableFuture<>();
            this.client.execute(post, new FutureCallback<HttpResponse>() {
                @Override
                public void completed(HttpResponse response) {
                    try (InputStream content = response.getEntity().getContent()) {
                        resultFuture.complete(objectMapper.readTree(EntityUtils.toString(response.getEntity())).get("result").asText());
                        response.getEntity().getContent().close();
                    } catch (Exception ex) { 
                        resultFuture.completeExceptionally(ex);
                    }
                }

                @Override
                public void failed(Exception excptn) {
                    resultFuture.completeExceptionally(excptn);
                }

                @Override
                public void cancelled() {
                    resultFuture.cancel(true);
                }
            });
            return resultFuture;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        
    }

    @Override
    public CompletionStage<Block> getBlock(String hash) {
        try {
            HttpPost post = new HttpPost(url);
            int verbosity = 2;
            ObjectNode body = objectMapper.createObjectNode().put("method", "getblock");
            body.putArray("params").add(hash).add(verbosity);
            post.setEntity(new StringEntity(objectMapper.writeValueAsString(body), JSON));
            CompletableFuture<Block> resultFuture = new CompletableFuture<>();
            this.client.execute(post, new FutureCallback<HttpResponse>() {
                @Override
                public void completed(HttpResponse response) {
                    try (InputStream content = response.getEntity().getContent()) {
                        JsonNode get = objectMapper.readTree(content).get("result");
                        response.getEntity().getContent().close();
                        resultFuture.complete(objectMapper.treeToValue(get, Block.class));
                    } catch (Exception ex) { 
                        resultFuture.completeExceptionally(ex);
                    }
                }

                @Override
                public void failed(Exception excptn) {
                    resultFuture.completeExceptionally(excptn);
                }

                @Override
                public void cancelled() {
                    resultFuture.cancel(true);
                }
            });
            return resultFuture;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        
    }

    @Override
    public CompletionStage<BlockHeader> getBlockHeader(String hash) {
        
        try {
            HttpPost post = new HttpPost(url);
            ObjectNode body = objectMapper.createObjectNode().put("method", "getblockheader");
            body.putArray("params").add(hash);
            post.setEntity(new StringEntity(objectMapper.writeValueAsString(body), JSON));
            //post.setHeader("Connection", "close");
            CompletableFuture<BlockHeader> resultFuture = new CompletableFuture<>();
            this.client.execute(post, new FutureCallback<HttpResponse>() {
                @Override
                public void completed(HttpResponse response) {
                    try (InputStream content = response.getEntity().getContent()) {
                        JsonNode get = objectMapper.readTree(EntityUtils.toString(response.getEntity())).get("result");
                        resultFuture.complete(objectMapper.treeToValue(get, BlockHeader.class));
                    } catch (Exception ex) { 
                        resultFuture.completeExceptionally(ex);
                    }
                }

                @Override
                public void failed(Exception excptn) {
                    resultFuture.completeExceptionally(excptn);
                }

                @Override
                public void cancelled() {
                    resultFuture.cancel(true);
                }
            });
            return resultFuture;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void close() {
        try {
            this.client.close();
        } catch (IOException ex) {
            Logger.getLogger(RpcClientImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
