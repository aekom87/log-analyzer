package com.anko.sparkdemo.model;

import lombok.Data;

/**
 * Created by Andrey on 07.12.2018.
 */
@Data(staticConstructor = "of")
public class HostLevelLogStat {
    private final HostLevelKey key;
    private final LogStat stat;
}
