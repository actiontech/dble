/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class FileUtils {

    private FileUtils() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(FileUtils.class);

    public static void makeParentDirs(final File file) throws IOException {
        File parent = Objects.requireNonNull(file, "file").getCanonicalFile().getParentFile();
        if (parent != null) {
            mkdir(parent, true);
        }
    }

    private static void mkdir(final File dir, final boolean createDirectoryIfNotExisting) throws IOException {
        if (!dir.exists()) {
            if (!createDirectoryIfNotExisting) {
                throw new IOException("The directory " + dir.getAbsolutePath() + " does not exist.");
            }
            if (!dir.mkdirs()) {
                throw new IOException("Could not create directory " + dir.getAbsolutePath());
            }
        }
        if (!dir.isDirectory()) {
            throw new IOException("File " + dir + " exists and is not a directory. Unable to create directory.");
        }
    }

    public static Integer getFileNextIndex(String name, String path, String suffix) {
        SortedMap<Integer, Path> eligibleFiles = new TreeMap<>();
        Pattern pattern = Pattern.compile(name + "(0?\\d+)" + suffix);
        DirectoryStream<Path> stream = null;
        try {
            File file = new File(path);
            makeParentDirs(file);
            stream = Files.newDirectoryStream(file.getParentFile().toPath());
            for (final Path entry : stream) {
                Matcher matcher = pattern.matcher(entry.toFile().getName());
                if (matcher.matches()) {
                    try {
                        Integer index = Integer.parseInt(matcher.group(1));
                        eligibleFiles.put(index, entry);
                    } catch (NumberFormatException ex) {
                        LOGGER.warn("Get file index exception：", ex);
                    }
                }
            }
        } catch (IOException ioe) {
            LOGGER.warn("Get file parentPath exception：", ioe);
        } finally {
            if (stream != null) {
                try {
                    stream.close();
                } catch (IOException e) {
                    LOGGER.warn("file close exception：", e);
                }
            }
        }
        return eligibleFiles.isEmpty() ? 1 : eligibleFiles.lastKey() + 1;
    }
}
