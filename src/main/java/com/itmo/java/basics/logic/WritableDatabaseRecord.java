package com.itmo.java.basics.logic;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.exceptions.Validator;

import javax.xml.crypto.Data;
import java.util.Arrays;

/**
 * Содержит информацию о параметрах {@link DatabaseRecord} для хранения в БД
 */
public interface WritableDatabaseRecord extends DatabaseRecord {

    /**
     * Возвращает размер ключа в байтах
     */
    int getKeySize();


    /**
     * Возвращает размер значения в байтах. -1, если значение отсутвует
     */
    int getValueSize();


    /**
     * Создает константную запись
     *
     * @return запись
     */
    static WritableDatabaseRecord createConstantRecord(byte[] key, byte[] value) throws DatabaseException {
        Validator.checkNull(key, "key");
        if (value == null) {
            return new ConstantWritableDatabaseRecord(Arrays.copyOf(key, key.length), null);
        } else {
            return new ConstantWritableDatabaseRecord(Arrays.copyOf(key, key.length), Arrays.copyOf(value, value.length));
        }
    }

    static WritableDatabaseRecord createDeleteRecord(byte[] key) {
        return new DeleteWritableDatabaseRecord(key);
    }


    class ConstantWritableDatabaseRecord implements WritableDatabaseRecord {
        private byte[] key;
        private byte[] value;

        private ConstantWritableDatabaseRecord(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public byte[] getKey() {
            return key;
        }

        @Override
        public byte[] getValue() {
            return value;
        }

        @Override
        public long size() {
            return key.length + value.length + 8; // key + value + 2*int
        }

        @Override
        public boolean isValuePresented() {
            return value != null;
        }

        @Override
        public int getKeySize() {
            return key != null ? key.length : 0;
        }

        @Override
        public int getValueSize() {
            return value != null ? value.length : 0;
        }
    }

    class DeleteWritableDatabaseRecord implements WritableDatabaseRecord {
        private byte[] key;

        private DeleteWritableDatabaseRecord(byte[] key) {
            this.key = key;
        }


        @Override
        public byte[] getKey() {
            return key;
        }

        @Override
        public byte[] getValue() {
            return new byte[0];
        }

        @Override
        public long size() {
            return key.length + 8;
        }

        @Override
        public boolean isValuePresented() {
            return false;
        }

        @Override
        public int getKeySize() {
            return key.length;
        }

        @Override
        public int getValueSize() {
            return 0;
        }
    }
}
