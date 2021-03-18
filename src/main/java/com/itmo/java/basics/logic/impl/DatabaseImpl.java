package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.exceptions.Validator;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.Table;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class DatabaseImpl implements Database {

    public static Database create(String dbName, Path databaseRoot) throws DatabaseException {
        Validator.checkNull(dbName, "dbName");
        Validator.checkNull(databaseRoot, "databaseRoot");
        return new DatabaseImpl(dbName, databaseRoot);
    }

    private String name;
    private Path rootPath;
    private Map<String, Table> tables = new HashMap<>();


    private DatabaseImpl(String name, Path rootPath) {
        this.name = name;
        this.rootPath = rootPath;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void createTableIfNotExists(String tableName) throws DatabaseException {
        if (tables.get(tableName) == null) {
            tables.put(tableName, TableImpl.create(tableName, rootPath, new TableIndex()));
        }
    }

    /**
     * Возвращает таблицу, если ее нет возвращает DatabaseException
     * @param tableName
     * @return Таблица
     * @throws DatabaseException если таблицы нет
     */
    private Table getTable(String tableName) throws DatabaseException {
        Table table = tables.get(tableName);
        if (table == null) {
            throw new DatabaseException("Table " + tableName + " is not exist");
        }
        return table;
    }

    @Override
    public void write(String tableName, String objectKey, byte[] objectValue) throws DatabaseException {
        Table table = getTable(tableName);
        table.write(objectKey, objectValue);
    }

    @Override
    public Optional<byte[]> read(String tableName, String objectKey) throws DatabaseException {
        return getTable(tableName).read(objectKey);
    }

    @Override
    public void delete(String tableName, String objectKey) throws DatabaseException {
        getTable(tableName).delete(objectKey);
    }
}
