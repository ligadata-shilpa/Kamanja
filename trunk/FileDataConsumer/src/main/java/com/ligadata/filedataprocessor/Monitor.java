package com.ligadata.filedataprocessor;

public interface Monitor {
    void init(String connectionConf, String monitoringConf);
    void monitor();
    void shutdown();
}
