package beam.dofns;

import avro.shaded.com.google.common.collect.Iterators;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.ByteBuddyDoFnInvokerFactoryHelper;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.graalvm.nativeimage.hosted.Feature;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.util.Iterator;

import static java.lang.reflect.Modifier.isAbstract;

public class PredefinedDoFnInvokerFeature implements Feature {
  private static final String REFLECT_CONFIG = "META-INF/native-image/reflect-config.json";
  private static final String PROXY_CONFIG = "META-INF/native-image/proxy-config.json";
  public static final String INVOKER_SUFFIX = "$DoFnInvoker";

  private final ObjectMapper mapper = new ObjectMapper();

  @Override
  public void afterRegistration(AfterRegistrationAccess access) {
    processConfig(REFLECT_CONFIG, new ReflectConfigProcessor());
    processConfig(PROXY_CONFIG, new ProxyConfigProcessor());
  }

  interface ConfigProcessor {
    /**
     * Process an entry of the config file, if returning {@code FALSE} the entry is afterwards
     * dropped once the config file is rewritten.
     */
    boolean process(ObjectNode entry);
  }

  static class ReflectConfigProcessor implements ConfigProcessor {
    @Override
    public boolean process(ObjectNode entry) {
      String name = entry.get("name").asText();

      if (name.startsWith("net.bytebuddy")) {
        return false; // Remove ByteBuddy from reflect config, not used at runtime anymore.
      }

      if (name.endsWith(INVOKER_SUFFIX)) {
        String doFnName = name.substring(0, name.length() - INVOKER_SUFFIX.length());
        // Generate the DoFn signature, the signature is kept in a static cache and available in the
        // native image.
        DoFnSignature signature = DoFnSignatures.getSignature(uninitializedClass(doFnName));
        // Dynamically generate and load invoker class using ByteBuddy / Beam.
        // This class is later added to the image based on reflect-config.json.
        ByteBuddyDoFnInvokerFactoryHelper.generateInvokerClass(signature);
        return true;
      }

      Class<?> cls = uninitializedClass(name);
      if (!(cls == null || isAbstract(cls.getModifiers())) && DoFn.class.isAssignableFrom(cls)) {
        // Make sure all required DoFn signatures are pre-generated
        DoFnSignatures.getSignature((Class) cls);
        // Remove from reflect config once generated
        return false;
      }
      return true;
    }
  }

  static class ProxyConfigProcessor implements ConfigProcessor {
    @Override
    public boolean process(ObjectNode entry) {
      Iterator<JsonNode> interfaces = entry.get("interfaces").elements();
      // Remove ByteBuddy interfaces, not used at runtime anymore.
      return !Iterators.all(interfaces, e -> e.asText().startsWith("net.bytebuddy"));
    }
  }

  private void processConfig(String config, ConfigProcessor processor) {
    URL configUrl = contextLoader().getResource(config);
    if (configUrl == null) {
      throw new RuntimeException("Configuration file " + config + " missing!");
    }

    ArrayNode resultConfig = mapper.createArrayNode();
    try (FileReader reader = new FileReader(configUrl.getFile())) {
      for (JsonNode entry : mapper.readTree(reader)) {
        if (processor.process((ObjectNode) entry)) {
          resultConfig.add(entry); // add entry to result config
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    try (FileWriter writer = new FileWriter(configUrl.getFile())) {
      mapper.writeValue(writer, resultConfig);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static <T> Class<T> uninitializedClass(String name) {
    try {
      return (Class) Class.forName(name, false, contextLoader());
    } catch (ClassNotFoundException e) {
      return null;
    }
  }

  private static ClassLoader contextLoader() {
    return Thread.currentThread().getContextClassLoader();
  }
}
