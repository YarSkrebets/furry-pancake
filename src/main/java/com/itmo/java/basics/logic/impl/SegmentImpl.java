package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.Validator;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.WritableDatabaseRecord;
import com.itmo.java.basics.logic.io.DatabaseInputStream;
import com.itmo.java.basics.logic.io.DatabaseOutputStream;

import javax.xml.crypto.Data;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;

public class SegmentImpl implements Segment {
    private static int DELETE_VALUE_SIZE_MARKER = -1;
    private static int SIZE_LIMIT = 100_000;

    static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {
        Validator.checkNull(segmentName, "segmentName");
        Validator.checkNull(tableRootPath, "tableRootPath");

        File file = new File(tableRootPath.toFile(), segmentName);
        try {
            file.createNewFile();
        } catch (IOException e) {
            throw new DatabaseException("Can't create segment file", e);
        }
        return new SegmentImpl(segmentName, file);
    }

    static Segment createByExistFile(String segmentName, Path pathToFile) {
        return new SegmentImpl(segmentName, pathToFile.toFile());
    }

    static String createSegmentName(String tableName) {
        return tableName + "_" + System.currentTimeMillis();
    }

    private String name;
    private File segmentFile;

    private SegmentImpl(String name, File segmentFile) {
        this.name = name;
        this.segmentFile = segmentFile;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws IOException {
        try (DatabaseInputStream inputStream = new DatabaseInputStream(new FileInputStream(segmentFile))) {
            Optional<DatabaseRecord> databaseRecord = inputStream.readDbUnit();
            while (databaseRecord.isPresent()) {
                DatabaseRecord record = databaseRecord.get();
                if (new String(record.getKey(), StandardCharsets.UTF_8).equals(objectKey)) {
                    return Optional.ofNullable(record.getValue());
                }
                databaseRecord = inputStream.readDbUnit();
            }
        }
        return Optional.empty();
    }

    @Override
    public boolean write(String objectKey, byte[] objectValue) throws IOException, DatabaseException {
        try (DatabaseOutputStream outputStream = new DatabaseOutputStream(new FileOutputStream(segmentFile, true))) {
            outputStream.write(WritableDatabaseRecord.createConstantRecord(objectKey.getBytes(StandardCharsets.UTF_8), objectValue));
        }
        return true;

    }

    @Override
    public boolean isReadOnly() {
        return segmentFile.length() > SIZE_LIMIT;
    }

    /**
     * Главная суть этого метода это просто удалить из индекса ключ, и после записать {keysize}{key}-1,
     * для того, чтобы потом можно было сделать индекс из этого
     */
    @Override
    public boolean delete(String objectKey) throws IOException {
        if (read(objectKey).isPresent()) {
            try (DatabaseOutputStream outputStream = new DatabaseOutputStream(new FileOutputStream(segmentFile, true))) {
                outputStream.write(WritableDatabaseRecord.createDeleteRecord(objectKey.getBytes(StandardCharsets.UTF_8)));
            }
            return true;
        } else {
            return false;
        }
    }
}
