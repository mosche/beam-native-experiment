package org.apache.beam.sdk.transforms.reflect;

import com.oracle.svm.core.annotate.Alias;
import com.oracle.svm.core.annotate.Delete;
import com.oracle.svm.core.annotate.RecomputeFieldValue;
import com.oracle.svm.core.annotate.RecomputeFieldValue.Kind;
import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.graalvm.nativeimage.hosted.FieldValueTransformer;

@TargetClass(DoFnSignatures.class)
public final class SubstituteDoFnSignatures {

  @Alias
  @RecomputeFieldValue(kind = Kind.Custom, declClass = MapTransformer.class, isFinal = true)
  private static Map<Class<? extends DoFn<?, ?>>, DoFnSignature> signatureCache;

  public static class MapTransformer implements FieldValueTransformer {
    @Override
    public Object transform(Object receiver, Object originalValue) {
      return new HashMap<>((Map) originalValue); // use HashMap for read-only access
    }
  }

  @Substitute
  public static <FnT extends DoFn<?, ?>> DoFnSignature getSignature(Class<FnT> fn) {
    DoFnSignature signature = signatureCache.get(fn); // don't call parseSignature if absent
    if(signature != null){
      return signature;
    }
    throw new IllegalStateException("Missing pre-generated DoFn signature for " + fn.getName());
  }

  @Delete
  private static DoFnSignature parseSignature(Class<? extends DoFn<?, ?>> fnClass) {
    return null;
  }
}
