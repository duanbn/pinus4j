package com.pinus.core.ser;

public interface Serializer
{

    byte[] ser(Object v, boolean isCompress) throws SerializeException;

    byte[] ser(Object v) throws SerializeException;

}
