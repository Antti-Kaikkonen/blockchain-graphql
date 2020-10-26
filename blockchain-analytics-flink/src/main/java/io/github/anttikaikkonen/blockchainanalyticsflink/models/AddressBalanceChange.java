package io.github.anttikaikkonen.blockchainanalyticsflink.models;

import com.datastax.driver.core.LocalDate;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AddressBalanceChange {
    private LocalDate date;
   
    private String address;
    
    private long balanceChange;
    
}
