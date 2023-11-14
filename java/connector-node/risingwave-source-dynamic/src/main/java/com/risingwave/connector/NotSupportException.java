package com.risingwave.connector;

import org.apache.flink.annotation.Public;

public class NotSupportException extends RuntimeException{
    public NotSupportException(){
        super("Don't support method or class");
    }
}
