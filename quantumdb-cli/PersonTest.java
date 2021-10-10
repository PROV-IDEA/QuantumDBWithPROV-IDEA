package io.quantumdb.cli.xml;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import io.quantumdb.cli.Person;

public class PersonTest {
    @Test
    public void aspectsAppliedTest() {
        assertEquals("Hello from Aspect!", Person.createHelloWorldStringTest());
    }

    @Test
    public void lombokApplied() {
        assertEquals("Person(name=John, age=32)", new Person("John", 32).toString());
    }
}
