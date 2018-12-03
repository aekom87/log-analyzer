package com.anko.sparkdemo.model;

import lombok.Data;

import java.io.Serializable;

/**
 * Created by Andrey on 03.12.2018.
 */
@Data(staticConstructor = "of")
public class LogStat implements Serializable {
    final private Integer count;
    final private Double rate;
}
