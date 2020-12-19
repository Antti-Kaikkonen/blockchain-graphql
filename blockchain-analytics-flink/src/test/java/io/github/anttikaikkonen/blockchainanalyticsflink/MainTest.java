package io.github.anttikaikkonen.blockchainanalyticsflink;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class MainTest {
    
    public MainTest() {
    }
    
    @BeforeAll
    public static void setUpClass() {
    }
    
    @AfterAll
    public static void tearDownClass() {
    }
    
    @BeforeEach
    public void setUp() {
    }
    
    @AfterEach
    public void tearDown() {
    }

    @Test
    public void testMain() throws Exception {
        System.out.println("main");
        String[] args = new String[2];
        args[0] = "--"+Main.PROPERTIES_TEST_MODE;
        args[1] = "true";
        Main.main(args);
        
    }


    
}
