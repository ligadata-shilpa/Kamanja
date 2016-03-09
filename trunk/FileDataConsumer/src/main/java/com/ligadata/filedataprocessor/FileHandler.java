package com.ligadata.filedataprocessor;
import com.ligadata.Exceptions.KamanjaException;

public interface FileHandler {
    String getFullPath();
    void openForRead() throws KamanjaException;
    int read(byte[] buf, int length) throws KamanjaException;
    void close();
    boolean moveTo(String newPath) throws KamanjaException;
    boolean delete() throws KamanjaException;
    long length() throws KamanjaException;
    long lastModified() throws KamanjaException;
}