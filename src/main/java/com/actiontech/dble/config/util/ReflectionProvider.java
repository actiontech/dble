/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.config.util;

import java.io.*;
import java.lang.reflect.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author mycat
 */
public class ReflectionProvider {

    private transient Map<Class<?>, byte[]> serializedDataCache = Collections.synchronizedMap(
            new HashMap<Class<?>, byte[]>());
    private transient FieldDictionary fieldDictionary = new FieldDictionary();

    public Object newInstance(Class<?> type) {
        try {
            Constructor<?>[] c = type.getDeclaredConstructors();
            for (Constructor<?> aC : c) {
                if (aC.getParameterTypes().length == 0) {
                    if (!Modifier.isPublic(aC.getModifiers())) {
                        aC.setAccessible(true);
                    }
                    return aC.newInstance();
                }
            }
            if (Serializable.class.isAssignableFrom(type)) {
                return instantiateUsingSerialization(type);
            } else {
                throw new ObjectAccessException("Cannot construct " + type.getName() +
                        " as it does not have a no-args constructor");
            }
        } catch (InstantiationException | IllegalAccessException e) {
            throw new ObjectAccessException("Cannot construct " + type.getName(), e);
        } catch (InvocationTargetException e) {
            if (e.getTargetException() instanceof RuntimeException) {
                throw (RuntimeException) e.getTargetException();
            } else if (e.getTargetException() instanceof Error) {
                throw (Error) e.getTargetException();
            } else {
                throw new ObjectAccessException("Constructor for " + type.getName() + " threw an exception",
                        e.getTargetException());
            }
        }
    }

    public void visitSerializableFields(Object object, Visitor visitor) {
        for (Iterator<Field> iterator = fieldDictionary.serializableFieldsFor(object.getClass()); iterator.hasNext(); ) {
            Field field = iterator.next();
            if (!fieldModifiersSupported(field)) {
                continue;
            }
            validateFieldAccess(field);
            try {
                Object value = field.get(object);
                visitor.visit(field.getName(), field.getType(), field.getDeclaringClass(), value);
            } catch (IllegalArgumentException | IllegalAccessException e) {
                throw new ObjectAccessException("Could not get field " + field.getClass() + "." + field.getName(), e);
            }
        }
    }

    public void writeField(Object object, String fieldName, Object value, Class<?> definedIn) {
        Field field = fieldDictionary.field(object.getClass(), fieldName, definedIn);
        validateFieldAccess(field);
        try {
            field.set(object, value);
        } catch (IllegalArgumentException | IllegalAccessException e) {
            throw new ObjectAccessException("Could not set field " + field.getName() + "@" + object.getClass(), e);
        }
    }

    public void invokeMethod(Object object, String methodName, Object value, Class<?> definedIn) {
        try {
            Method method = object.getClass().getMethod(methodName, value.getClass());
            method.invoke(object, value);
        } catch (Exception e) {
            throw new ObjectAccessException("Could not invoke " + object.getClass() + "." + methodName, e);
        }
    }

    public Class<?> getFieldType(Object object, String fieldName, Class<?> definedIn) {
        return fieldDictionary.field(object.getClass(), fieldName, definedIn).getType();
    }

    public boolean fieldDefinedInClass(String fieldName, Class<?> type) {
        try {
            Field field = fieldDictionary.field(type, fieldName, null);
            return fieldModifiersSupported(field);
        } catch (ObjectAccessException e) {
            return false;
        }
    }

    public Field getField(Class<?> definedIn, String fieldName) {
        return fieldDictionary.field(definedIn, fieldName, null);
    }

    private Object instantiateUsingSerialization(Class<?> type) {
        try {
            byte[] data;
            if (serializedDataCache.containsKey(type)) {
                data = serializedDataCache.get(type);
            } else {
                ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream stream = new DataOutputStream(bytes);
                stream.writeShort(ObjectStreamConstants.STREAM_MAGIC);
                stream.writeShort(ObjectStreamConstants.STREAM_VERSION);
                stream.writeByte(ObjectStreamConstants.TC_OBJECT);
                stream.writeByte(ObjectStreamConstants.TC_CLASSDESC);
                stream.writeUTF(type.getName());
                stream.writeLong(ObjectStreamClass.lookup(type).getSerialVersionUID());
                stream.writeByte(2); // classDescFlags (2 = Serializable)
                stream.writeShort(0); // field count
                stream.writeByte(ObjectStreamConstants.TC_ENDBLOCKDATA);
                stream.writeByte(ObjectStreamConstants.TC_NULL);
                data = bytes.toByteArray();
                serializedDataCache.put(type, data);
            }

            ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(data));
            return in.readObject();
        } catch (IOException e) {
            throw new ObjectAccessException("Cannot create " + type.getName() + " by JDK serialization", e);
        } catch (ClassNotFoundException e) {
            throw new ObjectAccessException("Cannot find class " + e.getMessage());
        }
    }

    private boolean fieldModifiersSupported(Field field) {
        return !(Modifier.isStatic(field.getModifiers()) || Modifier.isTransient(field.getModifiers()));
    }

    private void validateFieldAccess(Field field) {
        if (Modifier.isFinal(field.getModifiers())) {
            if (JVMInfo.is15()) {
                field.setAccessible(true);
            } else {
                throw new ObjectAccessException("Invalid final field " + field.getDeclaringClass().getName() + "." +
                        field.getName());
            }
        }
    }

    private Object readResolve() {
        serializedDataCache = Collections.synchronizedMap(new HashMap<Class<?>, byte[]>());
        fieldDictionary = new FieldDictionary();
        return this;
    }

}
