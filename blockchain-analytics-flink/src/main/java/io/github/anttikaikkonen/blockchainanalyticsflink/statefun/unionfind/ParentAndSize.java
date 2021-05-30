package io.github.anttikaikkonen.blockchainanalyticsflink.statefun.unionfind;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ParentAndSize {

    long size;
    String parent;
}
