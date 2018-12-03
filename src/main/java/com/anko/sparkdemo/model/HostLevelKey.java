package com.anko.sparkdemo.model;

import lombok.Data;

import java.io.Serializable;

/**
 * Created by Andrey on 03.12.2018.
 */
@Data(staticConstructor = "of")
public class HostLevelKey implements Serializable {
    final private String host;
    final private Log.Level level;
}
