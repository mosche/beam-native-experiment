package org.apache.beam.sdk.transforms.reflect;

import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;
import net.bytebuddy.description.type.TypeDescription;
import org.apache.beam.sdk.transforms.DoFn;

@TargetClass(ByteBuddyDoFnInvokerFactory.class)
public final class SubstituteByteBuddyDoFnInvokerFactory {

  @Substitute
  private static Class<? extends DoFnInvoker<?, ?>> generateInvokerClass(DoFnSignature signature) {
    Class<? extends DoFn<?, ?>> fnClass = signature.fnClass();

    String invokerName =
        StableInvokerNamingStrategy.forDoFnClass(fnClass)
            .withSuffix(DoFnInvoker.class.getSimpleName())
            .name(new TypeDescription.ForLoadedType(DoFnInvoker.class));
    try {
      return (Class) Class.forName(invokerName);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}
