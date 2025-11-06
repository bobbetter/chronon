package ai.chronon.service;

import ai.chronon.online.Api;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ai.chronon.api.ScalaJavaConversions;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;

/**
 * Responsible for loading the relevant concrete Chronon Api implementation and providing that
 * for use in the Web service code. We follow similar semantics as the Driver to configure this:
 * online.jar - Jar that contains the implementation of the Api
 * online.class - Name of the Api class
 * online.api.props - Structure that contains fields that are loaded and passed to the Api implementation
 * during instantiation to configure it (e.g. connection params)
 */
public class ApiProvider {
    private static final Logger logger = LoggerFactory.getLogger(ApiProvider.class);

    public static Api buildApi(ConfigStore configStore) throws Exception {
        configStore.validateOnlineApiConfig();

        // we've already validated and confirmed these are present
        String jarPath = configStore.getOnlineJar().get();
        String className = configStore.getOnlineClass().get();

        File jarFile = new File(jarPath);
        if (!jarFile.exists()) {
            throw new IllegalArgumentException("JAR file does not exist: " + jarPath);
        }

        logger.info("Loading API implementation from JAR: {}, class: {}", jarPath, className);

        // Create class loader for the API JAR
        URL jarUrl = jarFile.toURI().toURL();
        URLClassLoader apiClassLoader = new URLClassLoader(
                new URL[]{jarUrl},
                ApiProvider.class.getClassLoader()
        );

        // Load and instantiate the API implementation
        Class<?> apiClass = Class.forName(className, true, apiClassLoader);
        if (!Api.class.isAssignableFrom(apiClass)) {
            throw new IllegalArgumentException(
                    "Class " + className + " does not extend the Api abstract class"
            );
        }

        Map<String, String> propsMap = configStore.getOnlineApiProps();
        scala.collection.immutable.Map<String, String> scalaPropsMap = ScalaJavaConversions.toScala(propsMap);

        return (Api) apiClass.getConstructors()[0].newInstance(scalaPropsMap);
    }
}
