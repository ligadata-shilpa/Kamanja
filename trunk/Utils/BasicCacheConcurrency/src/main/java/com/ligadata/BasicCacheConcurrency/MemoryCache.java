package com.ligadata.BasicCacheConcurrency;

import java.util.Map;

/**
 * Created by Saleh on 3/15/2016.
 */
public interface MemoryCache {
    public void init();
    public void start();
    public void putInCache(Map map);
    public Object getFromCache(String key);
    public void shutdwon();
}
