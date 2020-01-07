/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;

/**
 * @author mycat
 */
public final class ObjectUtil {
    private ObjectUtil() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ObjectUtil.class);


    public static Object getStaticFieldValue(String className, String fieldName) {
        Class clazz = null;
        try {
            clazz = Class.forName(className);
            Field field = clazz.getField(fieldName);
            if (field != null) {
                return field.get(null);
            }
        } catch (ClassNotFoundException | IllegalAccessException | NoSuchFieldException e) {
            //ignore error
        }
        return null;
    }


    public static Object copyObject(Object object) {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        ObjectOutputStream s = null;
        try {
            s = new ObjectOutputStream(b);
            s.writeObject(object);
            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(b.toByteArray()));
            return ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

    }


    /**
     * compare the array.
     * <p>
     * </p>
     *
     * @param array1
     * @param array2
     * @return
     */
    public static boolean equals(Object array1, Object array2) {
        if (array1 == array2) {
            return true;
        }

        if ((array1 == null) || (array2 == null)) {
            return false;
        }

        Class<?> clazz = array1.getClass();

        if (!clazz.equals(array2.getClass())) {
            return false;
        }

        if (!clazz.isArray()) {
            return array1.equals(array2);
        }

        // array1 and array2 have same type
        if (array1 instanceof long[]) {
            return arrayEquals((long[]) array1, (long[]) array2);
        } else if (array1 instanceof int[]) {
            return arrayEquals((int[]) array1, (int[]) array2);
        } else if (array1 instanceof short[]) {
            return arrayEquals((short[]) array1, (short[]) array2);
        } else if (array1 instanceof byte[]) {
            return arrayEquals((byte[]) array1, (byte[]) array2);
        } else if (array1 instanceof double[]) {
            return arrayEquals((double[]) array1, (double[]) array2);
        } else if (array1 instanceof float[]) {
            return arrayEquals((float[]) array1, (float[]) array2);
        } else if (array1 instanceof boolean[]) {
            return arrayEquals((boolean[]) array1, (boolean[]) array2);
        } else if (array1 instanceof char[]) {
            return arrayEquals((char[]) array1, (char[]) array2);
        } else {
            return arrayEquals((Object[]) array1, (Object[]) array2);
        }
    }

    private static boolean arrayEquals(Object[] objectArray1, Object[] objectArray2) {

        if (objectArray1.length != objectArray2.length) {
            return false;
        }

        for (int i = 0; i < objectArray1.length; i++) {
            if (!equals(objectArray1[i], objectArray2[i])) {
                return false;
            }
        }

        return true;
    }

    private static boolean arrayEquals(char[] charArray1, char[] charArray2) {
        if (charArray1.length != charArray2.length) {
            return false;
        }

        for (int i = 0; i < charArray1.length; i++) {
            if (charArray1[i] != charArray2[i]) {
                return false;
            }
        }

        return true;
    }

    private static boolean arrayEquals(boolean[] booleanArray1, boolean[] booleanArray2) {

        if (booleanArray1.length != booleanArray2.length) {
            return false;
        }

        for (int i = 0; i < booleanArray1.length; i++) {
            if (booleanArray1[i] != booleanArray2[i]) {
                return false;
            }
        }

        return true;
    }

    private static boolean arrayEquals(float[] floatArray1, float[] floatArray2) {
        if (floatArray1.length != floatArray2.length) {
            return false;
        }

        for (int i = 0; i < floatArray1.length; i++) {
            if (Float.floatToIntBits(floatArray1[i]) != Float.floatToIntBits(floatArray2[i])) {
                return false;
            }
        }

        return true;
    }

    private static boolean arrayEquals(double[] doubleArray1, double[] doubleArray2) {
        if (doubleArray1.length != doubleArray2.length) {
            return false;
        }

        for (int i = 0; i < doubleArray1.length; i++) {
            if (Double.doubleToLongBits(doubleArray1[i]) != Double.doubleToLongBits(doubleArray2[i])) {
                return false;
            }
        }

        return true;
    }

    private static boolean arrayEquals(byte[] byteArray1, byte[] byteArray2) {
        if (byteArray1.length != byteArray2.length) {
            return false;
        }

        for (int i = 0; i < byteArray1.length; i++) {
            if (byteArray1[i] != byteArray2[i]) {
                return false;
            }
        }

        return true;
    }

    private static boolean arrayEquals(short[] shortArray1, short[] shortArray2) {
        if (shortArray1.length != shortArray2.length) {
            return false;
        }

        for (int i = 0; i < shortArray1.length; i++) {
            if (shortArray1[i] != shortArray2[i]) {
                return false;
            }
        }

        return true;
    }

    private static boolean arrayEquals(long[] longArray1, long[] longArray2) {
        if (longArray1.length != longArray2.length) {
            return false;
        }
        for (int i = 0; i < longArray1.length; i++) {
            if (longArray1[i] != longArray2[i]) {
                return false;
            }
        }
        return true;
    }

    private static boolean arrayEquals(int[] intArray1, int[] intArray2) {

        if (intArray1.length != intArray2.length) {
            return false;
        }

        for (int i = 0; i < intArray1.length; i++) {
            if (intArray1[i] != intArray2[i]) {
                return false;
            }
        }

        return true;
    }


    public static void copyProperties(Object fromObj, Object toObj) {
        Class<?> fromClass = fromObj.getClass();
        Class<?> toClass = toObj.getClass();

        try {
            BeanInfo fromBean = Introspector.getBeanInfo(fromClass);
            BeanInfo toBean = Introspector.getBeanInfo(toClass);

            PropertyDescriptor[] toPd = toBean.getPropertyDescriptors();
            List<PropertyDescriptor> fromPd = Arrays.asList(fromBean.getPropertyDescriptors());

            for (PropertyDescriptor propertyDescriptor : toPd) {
                propertyDescriptor.getDisplayName();
                PropertyDescriptor pd = fromPd.get(fromPd.indexOf(propertyDescriptor));
                if (pd.getDisplayName().equals(
                        propertyDescriptor.getDisplayName()) &&
                        !pd.getDisplayName().equals("class") &&
                        propertyDescriptor.getWriteMethod() != null) {
                    propertyDescriptor.getWriteMethod().invoke(toObj, pd.getReadMethod().invoke(fromObj, null));
                }

            }
        } catch (IntrospectionException | InvocationTargetException | IllegalAccessException | IllegalArgumentException e) {
            throw new RuntimeException(e);
        }
    }
}
