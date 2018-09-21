package net.jr.deebee;

import org.h2.Driver;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;

public class TestSimple {

    private static Connection getConnection() throws Exception {
        DriverManager.registerDriver(new Driver());
        return DriverManager.getConnection("jdbc:h2:mem:test");
    }

    @Test
    public void testFromScratch() throws Exception {


        UpdatePlanBuilder builder = new UpdatePlanBuilder();


        builder.fromVersion(UpdatePlanBuilder.INITIAL_VERSION)
                .toVersion("1.0.0")
                .action(updateRule -> {
                    System.out.println("to version 1, action 1");
                }).then(updateRule -> {
            System.out.println("to version 1, action 2");
        });


        builder.fromVersion("1.0.0")
                .toVersion("2.0.0")
                .action(updateRule -> {
                    System.out.println("to version 2, action 1");
                }).then(updateRule -> {
            System.out.println("to version 2, action 2");
        });

        UpdateRunner runner = builder.sqlRunner(getConnection());

        runner.run();


    }

}
