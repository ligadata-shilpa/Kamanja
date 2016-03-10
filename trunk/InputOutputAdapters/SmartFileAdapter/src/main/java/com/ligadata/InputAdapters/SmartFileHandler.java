package com.ligadata.InputAdapters;

import com.ligadata.Exceptions.KamanjaException;
/**
 * Created by Yasser on 3/10/2016.
 */
public interface SmartFileHandler {
    String getFullPath();
    void openForRead() throws KamanjaException;
    int read(byte[] buf, int length) throws KamanjaException;
    void close();
    boolean moveTo(String newPath) throws KamanjaException;
    boolean delete() throws KamanjaException;
    long length() throws KamanjaException;
    long lastModified() throws KamanjaException;
}
