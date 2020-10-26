/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.github.anttikaikkonen.blockchainanalyticsflink.statefun.unionfind;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class MergeOperation {
    String to;
    
    List<String> visited;
    
    AddTransactionOperation transaction;
}
