/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.config.util;

import com.actiontech.dble.config.ProblemReporter;
import com.actiontech.dble.util.BooleanUtil;
import com.actiontech.dble.util.StringUtil;
import com.actiontech.dble.util.SystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

/**
 * @author mycat
 */
public final class ParameterMapping {
    private ParameterMapping() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ParameterMapping.class);
    private static final Map<Class<?>, PropertyDescriptor[]> DESCRIPTORS = new HashMap<>();

    public static void mapping(Object object, Properties parameter, ProblemReporter problemReporter) throws IllegalAccessException,
            InvocationTargetException {
        PropertyDescriptor[] pds = getDescriptors(object.getClass());
        for (PropertyDescriptor pd : pds) {
            Object obj = parameter.get(pd.getName());
            Object value = obj;
            Class<?> cls = pd.getPropertyType();
            if (cls == null) {
                if (problemReporter != null) {
                    problemReporter.warn("unknown property [ " + pd.getName() + " ]");
                } else {
                    LOGGER.warn("unknown property [ " + pd.getName() + " ]");
                }
                continue;
            }

            if (obj instanceof String) {
                String valStr = ((String) obj).trim();
                if (!StringUtil.isEmpty(valStr)) {
                    valStr = ConfigUtil.filter(valStr);
                }
                if (isPrimitiveType(cls)) {
                    try {
                        value = convert(cls, valStr);
                    } catch (NumberFormatException nfe) {
                        if (problemReporter != null) {
                            problemReporter.warn("property [ " + pd.getName() + " ] '" + valStr + "' data type should be " + cls.toString() + "");
                        } else {
                            LOGGER.warn("property [ " + pd.getName() + " ] '" + valStr + "' data type should be " + cls.toString() + "");
                        }
                        parameter.remove(pd.getName());
                        continue;
                    }
                }
            }
            if (value != null) {
                Method method = pd.getWriteMethod();
                if (method != null) {
                    method.invoke(object, value);
                    parameter.remove(pd.getName());
                }
            }
        }
    }

    public static void mapping(Object object, Map<String, String> parameter, ProblemReporter problemReporter) throws IllegalAccessException,
            InvocationTargetException {
        PropertyDescriptor[] pds = getDescriptors(object.getClass());
        for (PropertyDescriptor pd : pds) {
            String valStr = parameter.get(pd.getName());
            Object value = valStr;
            Class<?> cls = pd.getPropertyType();
            if (cls == null) {
                if (problemReporter != null) {
                    problemReporter.warn("unknown property [ " + pd.getName() + " ]");
                } else {
                    LOGGER.warn("unknown property [ " + pd.getName() + " ]");
                }
                continue;
            }

            if (!StringUtil.isEmpty(valStr)) {
                valStr = ConfigUtil.filter(valStr);
                if (isPrimitiveType(cls)) {
                    try {
                        value = convert(cls, valStr);
                    } catch (NumberFormatException nfe) {
                        if (problemReporter != null) {
                            problemReporter.warn("property [ " + pd.getName() + " ] '" + valStr + "' data type should be " + cls.toString() + "");
                        } else {
                            LOGGER.warn("property [ " + pd.getName() + " ] '" + valStr + "' data type should be " + cls.toString() + "");
                        }
                        parameter.remove(pd.getName());
                        continue;
                    }
                }
            }
            if (value != null) {
                Method method = pd.getWriteMethod();
                if (method != null) {
                    method.invoke(object, value);
                    parameter.remove(pd.getName());
                }
            }
        }
    }


    public static Properties mapping(Object object, ProblemReporter problemReporter) throws IllegalAccessException,
            InvocationTargetException {
        Properties systemProperties = (Properties) (System.getProperties().clone());
        for (String key : SystemProperty.getInnerProperties()) {
            systemProperties.remove(key);
        }
        PropertyDescriptor[] pds = getDescriptors(object.getClass());
        for (PropertyDescriptor pd : pds) {
            String propertyName = pd.getName();
            String valStr = systemProperties.getProperty(propertyName);
            if (valStr == null) {
                continue;
            }
            Object value = valStr;
            Class<?> cls = pd.getPropertyType();
            if (cls == null) {
                String msg = "unknown property [ " + pd.getName() + " ]";
                if (problemReporter != null) {
                    problemReporter.warn(msg);
                } else {
                    LOGGER.warn(msg);
                }
                continue;
            }
            valStr = valStr.trim();
            if (!StringUtil.isEmpty(valStr)) {
                valStr = ConfigUtil.filter(valStr);
            }
            if (isPrimitiveType(cls)) {
                try {
                    value = convert(cls, valStr);
                } catch (NumberFormatException nfe) {
                    String msg = "property [ " + pd.getName() + " ] '" + valStr + "' data type should be " + cls.toString();
                    if (problemReporter != null) {
                        problemReporter.warn(msg);
                    } else {
                        LOGGER.warn(msg);
                    }
                    systemProperties.remove(propertyName);
                    continue;
                }
            }
            if (value != null) {
                Method method = pd.getWriteMethod();
                if (method != null) {
                    method.invoke(object, value);
                }
            }
            systemProperties.remove(propertyName);
        }
        return systemProperties;
    }

    /**
     * @param clazz
     * @return
     */
    private static PropertyDescriptor[] getDescriptors(Class<?> clazz) {
        PropertyDescriptor[] pds;
        List<PropertyDescriptor> list;
        PropertyDescriptor[] pds2 = DESCRIPTORS.get(clazz);
        //clazz init for first time?
        if (null == pds2) {
            try {
                BeanInfo beanInfo = Introspector.getBeanInfo(clazz);
                pds = beanInfo.getPropertyDescriptors();
                list = new ArrayList<>();
                //add property it type is not null
                for (PropertyDescriptor pd : pds) {
                    if (null != pd.getPropertyType()) {
                        list.add(pd);
                    }
                }
                pds2 = new PropertyDescriptor[list.size()];
                list.toArray(pds2);
            } catch (IntrospectionException ie) {
                LOGGER.info("ParameterMappingError", ie);
                pds2 = new PropertyDescriptor[0];
            }
        }
        DESCRIPTORS.put(clazz, pds2);
        return (pds2);
    }

    private static Object convert(Class<?> cls, String string) {
        Method method = null;
        Object value = null;
        if (cls.equals(String.class)) {
            value = string;
        } else if (cls.equals(Boolean.TYPE)) {
            value = BooleanUtil.parseBoolean(string);
        } else if (cls.equals(Byte.TYPE)) {
            value = Byte.valueOf(string);
        } else if (cls.equals(Short.TYPE)) {
            value = Short.valueOf(string);
        } else if (cls.equals(Integer.TYPE)) {
            value = Integer.valueOf(string);
        } else if (cls.equals(Long.TYPE)) {
            value = Long.valueOf(string);
        } else if (cls.equals(Double.TYPE)) {
            value = Double.valueOf(string);
        } else if (cls.equals(Float.TYPE)) {
            value = Float.valueOf(string);
        } else if ((cls.equals(Boolean.class)) || (cls.equals(Byte.class)) || (cls.equals(Short.class)) ||
                (cls.equals(Integer.class)) || (cls.equals(Long.class)) || (cls.equals(Float.class)) ||
                (cls.equals(Double.class))) {
            try {
                method = cls.getMethod("valueOf", String.class);
                value = method.invoke(null, string);
            } catch (Exception t) {
                LOGGER.info("valueofError", t);
                value = null;
            }
        } else if (cls.equals(Class.class)) {
            try {
                value = Class.forName(string);
            } catch (ClassNotFoundException e) {
                throw new ConfigException(e);
            }
        } else {
            value = null;
        }
        return (value);
    }

    private static boolean isPrimitiveType(Class<?> cls) {
        return cls.equals(String.class) || cls.equals(Boolean.TYPE) || cls.equals(Byte.TYPE) || cls.equals(Short.TYPE) ||
                cls.equals(Integer.TYPE) || cls.equals(Long.TYPE) || cls.equals(Double.TYPE) ||
                cls.equals(Float.TYPE) || cls.equals(Boolean.class) || cls.equals(Byte.class) ||
                cls.equals(Short.class) || cls.equals(Integer.class) || cls.equals(Long.class) ||
                cls.equals(Float.class) || cls.equals(Double.class) || cls.equals(Class.class);
    }

}
