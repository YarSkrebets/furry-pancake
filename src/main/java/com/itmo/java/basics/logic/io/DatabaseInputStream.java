package com.itmo.java.basics.logic.io;

import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.WritableDatabaseRecord;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

/**
 * Класс, отвечающий за чтение данных из БД
 */
public class DatabaseInputStream extends DataInputStream {
    private static final int REMOVED_OBJECT_SIZE = -1;

    public DatabaseInputStream(InputStream inputStream) {
        super(inputStream);
    }

    /**
     * Читает следующую запись (см {@link DatabaseOutputStream#write(WritableDatabaseRecord)})
     *
     * @return следующую запись, если она существует. {@link Optional#empty()} - если конец файла достигнут
     */
    public Optional<DatabaseRecord> readDbUnit() throws IOException {
        int keySize = 0;
        try {
            keySize = readInt();
        } catch (EOFException e) {
            return Optional.empty();
        }

        byte[] buffer = new byte[keySize];
        super.readFully(buffer);
        byte[] key = buffer;

        int valueSize = readInt();
        byte[] value = null;
        if (valueSize > 0) {
            buffer = new byte[valueSize];
            super.readFully(buffer);
            value = buffer;
        }

        return Optional.of(DatabaseRecord.createConstantRecord(key, value));
    }

}
