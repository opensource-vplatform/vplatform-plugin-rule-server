package com.toone.v3.platform.rule.model;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * DeleteOnCloseFileInputStreamWrapper
 *
 * 在流关闭时删除文件，用于临时文件的自动删除
 *
 * @author wangbin
 * @createDate 2012-2-25
 */
public class FileInputStreamWrapper extends FileInputStream {

    /**
     * 可自动删除的文件
     */
    private File file;

    public FileInputStreamWrapper(File file) throws FileNotFoundException {
        super(file);
        this.file = file;
    }

    @Override
    public void close() throws IOException {
        try {
            super.close();
        } finally {
            if (file != null && file.exists()) file.delete();
        }
    }
}

