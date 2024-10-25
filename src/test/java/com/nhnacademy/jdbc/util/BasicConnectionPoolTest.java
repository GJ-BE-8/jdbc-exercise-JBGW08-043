package com.nhnacademy.jdbc.util;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;

import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(value = MethodOrderer.OrderAnnotation.class)
class BasicConnectionPoolTest {

    static BasicConnectionPool basicConnectionPool;
    static Connection connection1;
    static Connection connection2;
    static Connection connection3;
    static Connection connection4;
    static Connection connection5;

    @BeforeAll
    static void setUp() {
        //todo#0 - jdbcUrl, username, password를 설정하세요
        basicConnectionPool = new BasicConnectionPool(com.mysql.cj.jdbc.Driver.class.getName(),"jdbc:mysql://133.186.241.167:3306/nhn_academy_43","nhn_academy_43","nBsX6gXJ7loHtMn/",5);
    }

    @AfterAll
    static void connectionClose() throws SQLException {
        basicConnectionPool.distory();
    }

    @Test
    @Order(0)
    @DisplayName("Driver not found Exception")
    void init(){
        Assertions.assertThrows(RuntimeException.class,
            ()-> new BasicConnectionPool("org.mariadb.jdbc.Driver","jdbcUrl","userName","password",5)
        );
    }

    @Test
    @Order(1)
    @DisplayName("get connection")
    void getConnection() throws InterruptedException, SQLException {
        connection1 = basicConnectionPool.getConnection();
        connection2 = basicConnectionPool.getConnection();
        connection3 = basicConnectionPool.getConnection();
        Assertions.assertAll(
                ()->Assertions.assertTrue(connection1.isValid(1000)),
                ()->Assertions.assertTrue(connection2.isValid(1000)),
                ()->Assertions.assertTrue(connection3.isValid(1000)),
                ()->Assertions.assertEquals(basicConnectionPool.getUsedConnectionSize(),3)
        );
    }

    @Disabled
    @Test
    @Order(2)
    @DisplayName("empty connection-pool")
    void getConnection_empty() throws InterruptedException, SQLException {
        connection4 = basicConnectionPool.getConnection();
        connection5 = basicConnectionPool.getConnection();
        Connection connection6 = basicConnectionPool.getConnection();

        Assertions.assertAll(
                ()->Assertions.assertEquals(basicConnectionPool.getUsedConnectionSize(),5)
        );

    }

    @Test
    @Order(3)
    @DisplayName("release connection")
    void releaseConnection() {
        basicConnectionPool.releaseConnection(connection1);
        basicConnectionPool.releaseConnection(connection2);
        basicConnectionPool.releaseConnection(connection3);

        Assertions.assertEquals(basicConnectionPool.getUsedConnectionSize(),0);
    }
}