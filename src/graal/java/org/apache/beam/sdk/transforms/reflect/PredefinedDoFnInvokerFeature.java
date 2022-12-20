package org.apache.beam.sdk.transforms.reflect;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Paths;
import org.apache.beam.sdk.transforms.DoFn;
import org.graalvm.nativeimage.hosted.Feature;
import org.graalvm.nativeimage.hosted.RuntimeReflection;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.Index;
import org.jboss.jandex.IndexReader;

public class PredefinedDoFnInvokerFeature implements Feature {

  @Override
  public void beforeAnalysis(BeforeAnalysisAccess access) {
    File jandex = Paths.get("../../jandex/jandexMain/jandex.idx").toFile();

    try (FileInputStream jis = new FileInputStream(jandex)) {
      Index index = new IndexReader(jis).read();
      for (ClassInfo delegate : index.getAllKnownSubclasses(DoFn.class)) {

        Class<?> clazz = Class.forName(delegate.name().toString(), false, contextLoader());
        DoFnSignature signature = DoFnSignatures.getSignature((Class) clazz);

        // Dynamically generate invoker class using ByteBuddy / Beam.
        Class<?> invokerClass = ByteBuddyDoFnInvokerFactory.generateInvokerClass(signature);

        // Register the invoker class and constructor for inclusion in the image.
        RuntimeReflection.register(invokerClass);
        RuntimeReflection.register(invokerClass.getConstructor(clazz));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private ClassLoader contextLoader() {
    return Thread.currentThread().getContextClassLoader();
  }
}
