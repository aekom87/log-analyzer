package com.anko.sparkdemo.model;

import lombok.Data;

import java.io.Serializable;

/**
 * Created by Andrey on 03.12.2018.
 */
@Data
public class Log implements Serializable {
    private Long timestamp;
    private String host;
    private Level level;
    private String text;

    public enum Level {
        TRACE,
        DEBUG,
        INFO,
        WARN,
        ERROR
    }
}
