package com.itmo.java.basics.exceptions;

import com.itmo.java.basics.logic.Database;

public class Validator {
    public static void checkNull(String string, String fieldName) throws DatabaseException {
        if (string == null || string.isEmpty()) {
            throw new DatabaseException(fieldName + " is null");
        }
    }

    public static void checkNull(Object object, String fieldName) throws DatabaseException {
        if (object == null) {
            throw new DatabaseException(fieldName + " is null");
        }
    }



    private Validator() {}
}
