package io.github.anttikaikkonen.blockchainanalyticsflink.statefun.unionfind;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class MergeRootOperation {
    String root;
    long rootSize;
    List<String> rootVisited;
    
    List<String> visited;
    
    AddTransactionOperation transaction;
}
