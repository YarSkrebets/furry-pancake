package com.itmo.java.basics.logic;

import java.util.Arrays;

/**
 * Представляет собой единицу хранения в БД
 */
public interface DatabaseRecord {
    /**
     * Возвращает ключ
     */
    byte[] getKey();

    /**
     * Возвращает значение
     */
    byte[] getValue();

    /**
     * Возвращает размер хранимой записи в базе данных. Используется для определения offset (сдвига)
     */
    long size();

    /**
     * Индикатор, есть ли значение
     */
    boolean isValuePresented();

    /**
     * Создает константную запись
     *
     * @return запись
     */
    static DatabaseRecord createConstantRecord(byte[] key, byte[] value) {
        if (value == null) {
            return new ConstantDatabaseRecord(Arrays.copyOf(key, key.length), null);
        } else {
            return new ConstantDatabaseRecord(Arrays.copyOf(key, key.length), Arrays.copyOf(value, value.length));
        }
    }

    class ConstantDatabaseRecord implements DatabaseRecord {
        private byte[] key;
        private byte[] value;

        private ConstantDatabaseRecord(byte[] key, byte[] value) {
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
        public String toString() {
            return "ConstantDatabaseRecord{" +
                    "key=" + Arrays.toString(key) +
                    ", value=" + Arrays.toString(value) +
                    '}';
        }
    }

}

