package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.Table;
import sun.misc.SharedSecrets;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TableImpl implements Table {
    private String tableName;
    private Path pathToDatabaseRoot;
    private TableIndex tableIndex;
    private List<Segment> segments = new ArrayList<>();
    private Segment lastSegment;

    static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        TableImpl current = new TableImpl(tableName, pathToDatabaseRoot, tableIndex);
        current.initSegments();
        return current;
    }

    private TableImpl(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) {
        this.tableName = tableName;
        this.pathToDatabaseRoot = pathToDatabaseRoot;
        this.tableIndex = tableIndex;
    }

    @Override
    public String getName() {
        return tableName;
    }

    private void initSegments() throws DatabaseException {
        try (Stream<Path> paths = Files.walk(pathToDatabaseRoot)) {
            List<Path> segmentPathes = paths
                    .filter(Files::isRegularFile)
                    .filter(path -> path.startsWith(tableName))
                    .sorted(Comparator.comparing(Path::getFileName))
                    .collect(Collectors.toList());
            for (Path path : segmentPathes) {
                Segment createdSegment = SegmentImpl.create(path.getFileName().toString(), path);
                this.segments.add(createdSegment);
                this.lastSegment = createdSegment;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private Segment createNewSegment() throws DatabaseException {
        Segment segment = SegmentImpl.create(SegmentImpl.createSegmentName(tableName), pathToDatabaseRoot);
        segments.add(segment);
        return segment;
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        try {
            Segment segment = tableIndex.searchForKey(objectKey).orElse(null);
            if (segment != null) {
                return segment.read(objectKey);
            } else {
                return Optional.empty();
            }
        } catch (IOException e) {
            throw new DatabaseException("Unexpected IOException", e);
        }
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        if ((lastSegment == null) || (lastSegment.isReadOnly())) {
            lastSegment = createNewSegment();
        }
        try {
            lastSegment.write(objectKey, objectValue);
            tableIndex.onIndexedEntityUpdated(objectKey, lastSegment);
        } catch (IOException e) {
            throw new DatabaseException("Can't write value, because IO", e);
        }
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        Optional<Segment> segment = tableIndex.searchForKey(objectKey);
        if (segment.isPresent()) {
            try {
                if (!segment.get().delete(objectKey)) {
                    throw new DatabaseException("Не удалось найти запись");
                }
            } catch (IOException e) {
                throw new DatabaseException("Ошибка ввода-вывода", e);
            }
            tableIndex.onIndexedEntityUpdated(objectKey, null);

        } else {
            throw new DatabaseException("Не удалось найти запись");
        }
    }
}
