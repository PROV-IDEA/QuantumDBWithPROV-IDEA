package io.quantumdb.cli;



import lombok.Value;

@Value
public class Person {
    String name;
    int age;

    public static String createHelloWorldStringTest() {
        return "Hello, World!";
    }
}
