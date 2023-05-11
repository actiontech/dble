/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.config.util;

import com.actiontech.dble.config.ProblemReporter;
import com.actiontech.dble.util.BooleanUtil;
import com.actiontech.dble.util.StringUtil;
import com.actiontech.dble.util.SystemProperty;
import com.google.common.base.Strings;
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
    private static List<String> errorParameters = new ArrayList<>();
    private static final Map<String, String> COMPATIBLE_MAP = new HashMap<>();
    private static Set<String> errorCompatibleSet = new HashSet<>();
    private static final Set<String> ON_OFF_SET = new HashSet<>();


    static {
        COMPATIBLE_MAP.put("complexQueryWorker", "complexExecutor");
        COMPATIBLE_MAP.put("NIOFrontRW", "processors");
        COMPATIBLE_MAP.put("NIOBackendRW", "backendProcessors");
        COMPATIBLE_MAP.put("frontWorker", "processorExecutor");
        COMPATIBLE_MAP.put("backendWorker", "backendProcessorExecutor");
        COMPATIBLE_MAP.put("writeToBackendWorker", "writeToBackendExecutor");


        ON_OFF_SET.add("useCompression");
        ON_OFF_SET.add("usingAIO");
        ON_OFF_SET.add("useThreadUsageStat");
        ON_OFF_SET.add("usePerformanceMode");
        ON_OFF_SET.add("useCostTimeStat");
        ON_OFF_SET.add("autocommit");
        ON_OFF_SET.add("checkTableConsistency");
        ON_OFF_SET.add("recordTxn");
        ON_OFF_SET.add("frontSocketNoDelay");
        ON_OFF_SET.add("backSocketNoDelay");
        ON_OFF_SET.add("enableGeneralLog");
        ON_OFF_SET.add("enableBatchLoadData");
        ON_OFF_SET.add("enableRoutePenetration");
        ON_OFF_SET.add("enableAlert");
        ON_OFF_SET.add("enableStatistic");
        ON_OFF_SET.add("enableStatisticAnalysis");
        ON_OFF_SET.add("enableSessionActiveRatioStat");
        ON_OFF_SET.add("enableConnectionAssociateThread");
        ON_OFF_SET.add("enableAsyncRelease");
        ON_OFF_SET.add("enableMemoryBufferMonitor");
        ON_OFF_SET.add("enableMemoryBufferMonitorRecordPool");
        ON_OFF_SET.add("enableSlowLog");
        ON_OFF_SET.add("useCostTimeStat");

        ON_OFF_SET.add("capClientFoundRows");
        ON_OFF_SET.add("useJoinStrategy");
        ON_OFF_SET.add("enableCursor");
        ON_OFF_SET.add("enableFlowControl");
        ON_OFF_SET.add("useOuterHa");
        ON_OFF_SET.add("useNewJoinOptimizer");
        ON_OFF_SET.add("inSubQueryTransformToJoin");
        ON_OFF_SET.add("closeHeartBeatRecord");
    }

    public static void mapping(Object target, Properties src, ProblemReporter problemReporter) throws IllegalAccessException,
            InvocationTargetException {
        PropertyDescriptor[] pds = getDescriptors(target.getClass());
        for (PropertyDescriptor pd : pds) {
            String name = pd.getName();
            String valStr = src.getProperty(name);
            valStr = compatibleProcess(name, valStr, src);
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
            if (valStr == null) {
                continue;
            }
            if (!StringUtil.isEmpty(valStr)) {
                valStr = ConfigUtil.filter(valStr.trim());
            }
            if (isPrimitiveType(cls)) {
                try {
                    valStr = onOffProcess(name, cls, valStr);
                    value = convert(cls, valStr);
                } catch (NumberFormatException nfe) {
                    String propertyName = pd.getName();
                    String message = getTypeErrorMessage(propertyName, valStr, cls);
                    if (problemReporter != null) {
                        problemReporter.warn(message);
                        errorParameters.add(message);
                    } else {
                        LOGGER.warn(message);
                    }
                    src.remove(propertyName);
                    continue;
                }
            }
            if (value != null) {
                Method method = pd.getWriteMethod();
                if (method != null) {
                    method.invoke(target, value);
                    src.remove(pd.getName());
                }
            }
        }
    }

    public static Properties mappingFromSystemProperty(Object target, ProblemReporter problemReporter) throws IllegalAccessException,
            InvocationTargetException {
        Properties systemProperties = (Properties) (System.getProperties().clone());
        for (String key : SystemProperty.getInnerProperties()) {
            systemProperties.remove(key);
        }
        PropertyDescriptor[] pds = getDescriptors(target.getClass());
        for (PropertyDescriptor pd : pds) {
            String propertyName = pd.getName();
            String valStr = systemProperties.getProperty(propertyName);
            valStr = compatibleProcess(propertyName, valStr, systemProperties);
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
                    valStr = onOffProcess(propertyName, cls, valStr);
                    value = convert(cls, valStr);
                } catch (NumberFormatException nfe) {
                    String msg = getTypeErrorMessage(propertyName, valStr, cls);
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
                    method.invoke(target, value);
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

    private static Object convert(Class<?> cls, String string) throws NumberFormatException, IllegalStateException {
        Method method = null;
        Object value;
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
                if (t instanceof InvocationTargetException) {
                    final Throwable targetException = ((InvocationTargetException) t).getTargetException();
                    if (targetException instanceof RuntimeException) {
                        //include NumberFormatException
                        throw (RuntimeException) targetException;
                    }
                }
                // won't happen generally,If it still happens ,maybe throw an exception and stop process  is a good idea.
                LOGGER.info("valueofError", t);
                throw new IllegalStateException(t);
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

    public static void checkMappingResult() {
        if (!errorParameters.isEmpty()) {
            String[] errorArray = new String[errorParameters.size()];
            errorParameters.toArray(errorArray);
            errorParameters.clear();
            throw new ConfigException("These properties of system are not recognized: " + StringUtil.join(errorArray, ","));
        }
    }

    private static boolean isPrimitiveType(Class<?> cls) {
        return cls.equals(String.class) || cls.equals(Boolean.TYPE) || cls.equals(Byte.TYPE) || cls.equals(Short.TYPE) ||
                cls.equals(Integer.TYPE) || cls.equals(Long.TYPE) || cls.equals(Double.TYPE) ||
                cls.equals(Float.TYPE) || cls.equals(Boolean.class) || cls.equals(Byte.class) ||
                cls.equals(Short.class) || cls.equals(Integer.class) || cls.equals(Long.class) ||
                cls.equals(Float.class) || cls.equals(Double.class) || cls.equals(Class.class);
    }


    public static String compatibleProcess(String name, String valStr, Properties src) {
        if (COMPATIBLE_MAP.containsKey(name)) {
            String values = COMPATIBLE_MAP.get(name);
            String compatibleVal = src.getProperty(values);
            src.remove(values);
            if (!Strings.isNullOrEmpty(compatibleVal) && Strings.isNullOrEmpty(valStr)) {
                valStr = compatibleVal;
                errorCompatibleSet.add(name);
                LOGGER.warn("property [ " + values + " ] has been replaced by the property [ " + name + " ], you are recommended to use property [ " + name +
                        " ] to replace the property [  " + values + " ] ");
            }
        }
        return valStr;
    }

    private static String onOffProcess(String name, Class<?> cls, String val) {
        String value = val;
        if (!ON_OFF_SET.contains(name)) {
            return value;
        }
        if (!cls.equals(Integer.TYPE) && !cls.equals(Boolean.TYPE)) {
            throw new NumberFormatException("parameter " + name + " is not boolean value or Integer value");
        }
        int valInteger;
        boolean valBoolean;
        if (BooleanUtil.isBoolean(val)) {
            valBoolean = BooleanUtil.parseBoolean(val);
            valInteger = booleanToInt(valBoolean);
        } else {
            valInteger = Integer.parseInt(val);
            checkOnOffInteger(valInteger);
            valBoolean = intToBoolean(valInteger);
        }
        if (cls.equals(Integer.TYPE)) {
            value = String.valueOf(valInteger);
        } else {
            value = String.valueOf(valBoolean);
        }
        return value;
    }

    private static void checkOnOffInteger(int valInteger) {
        if (valInteger < 0 || valInteger > 1) {
            throw new NumberFormatException("value " + valInteger + " is illegal");
        }
    }

    public static String getErrorCompatibleMessage(String name) {
        String message = "";
        if (errorCompatibleSet.contains(name)) {
            String oldName = COMPATIBLE_MAP.get(name);
            message = "property [ " + oldName + " ] has been replaced by the property [ " + name + " ].  ";
        }
        return message;
    }

    private static String getTypeErrorMessage(String name, String values, Class<?> cls) {
        String message = getErrorCompatibleMessage(name);
        StringBuilder sb = new StringBuilder(message);
        if (ON_OFF_SET.contains(name)) {
            sb.append("check the property [ ").append(name).append(" ] '").append(values).append("' data type or value");
        } else {
            sb.append("property [ ").append(name).append(" ] '").append(values).append("' data type should be ").append(cls.toString());
        }
        return sb.toString();
    }

    private static boolean intToBoolean(int num) {
        return num != 0;
    }

    private static int booleanToInt(boolean bool) {
        return bool ? 1 : 0;
    }

}
