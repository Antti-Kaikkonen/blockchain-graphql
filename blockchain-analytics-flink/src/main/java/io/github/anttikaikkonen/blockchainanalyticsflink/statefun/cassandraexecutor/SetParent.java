package io.github.anttikaikkonen.blockchainanalyticsflink.statefun.cassandraexecutor;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class SetParent {
    String parent;
}
