package com.demo.spring_cache_ignite.constant;

public class IgniteConstants {
    public static final String PROP_BEGIN = "${";
    public static final String PROP_END = "}";
    public static final String IGNITE_PROPERTY_PREFIX = "spring.cache.jcache";
    public static final String IGNITE_CONFIG_PROP_SPRING = PROP_BEGIN + IGNITE_PROPERTY_PREFIX + ".config" + PROP_END;
    public static final String IGNITE_CONFIG_PROPS_PREFIX = IGNITE_PROPERTY_PREFIX + ".ignite";
    public static final String IGNITE_LOG_PERFORMANCE_PROP = IGNITE_CONFIG_PROPS_PREFIX + ".log-performance";
    public static final String IGNITE_SYSTEM_PROPERTIES_CONFIGURATION = "igniteSystemPropertiesConfiguration";
    public static final String IGNITE_CONFIG_XML_ABSOLUTE_PATH = "C:\\Code\\Fusion\\spring-cache-ignite\\src\\main\\resources\\cache\\example-ignite.xml";
}
