package com.pinus.test;

public enum EnumTest
{
    A("1", "a"), B("2", "b");

    private String name;
    private String code;

    private EnumTest(String name, String code)
    {
        this.name = name;
        this.code = code;
    }
}
