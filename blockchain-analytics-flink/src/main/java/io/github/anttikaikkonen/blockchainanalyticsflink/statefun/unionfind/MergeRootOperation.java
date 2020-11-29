package io.github.anttikaikkonen.blockchainanalyticsflink.statefun.unionfind;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class MergeRootOperation {
    String root;
    long rootSize;
    
    boolean calledFromChild;
    //String rootFrom;
    
    //String from;
    //List<String> rootVisited;
    
    //List<String> visited;
    
    //AddTransactionOperation transaction;
}
