package org.apache.beam.sdk.transforms.reflect;

import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;
import org.apache.beam.sdk.transforms.DoFn;

@TargetClass(ByteBuddyDoFnInvokerFactory.class)
public final class SubstituteByteBuddyDoFnInvokerFactory {

  @Substitute
  private static Class<? extends DoFnInvoker<?, ?>> generateInvokerClass(DoFnSignature signature) {
    Class<? extends DoFn<?, ?>> fnClass = signature.fnClass();
    try {
      return (Class) Class.forName(fnClass.getName() + PredefinedDoFnInvokerFeature.INVOKER_SUFFIX);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}
