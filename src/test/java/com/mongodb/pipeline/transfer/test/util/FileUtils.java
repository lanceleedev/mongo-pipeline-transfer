package com.mongodb.pipeline.transfer.test.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019/9/16     Create this file
 * </pre>
 */
public final class FileUtils {
    private FileUtils() {
    }

    /**
     * 读取目录内所有文件内容
     * @param path
     * @return
     */
    public static Map<String, String> readDirToString(String path) {
        Map<String, String> result = null;
        File dir = new File(path);
        if (dir.isDirectory()) {
            String[] files = dir.list();
            result = new HashMap<String, String>((int) (files.length / 0.75));
            if (!path.endsWith(File.separator)) {
                path += File.separator;
            }
            for (int i = 0; i < files.length; i++) {
                String filename = path + files[i];
                result.put(filename, readFileToString(filename));
            }
        }
        return result;
    }

    /**
     * 读取文件内容
     * @param fileName
     * @return
     */
    public static String readFileToString(String fileName) {
        FileInputStream fis = null;
        try {
            File file = new File(fileName);
            byte[] filecontent = new byte[(int) file.length()];
            fis = new FileInputStream(file);
            fis.read(filecontent);
            return new String(filecontent);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (null != fis) {
                try {
                    fis.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }
}
