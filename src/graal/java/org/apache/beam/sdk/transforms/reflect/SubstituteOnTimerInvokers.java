package org.apache.beam.sdk.transforms.reflect;

import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.OnTimerInvoker;
import org.apache.beam.sdk.transforms.reflect.OnTimerInvokers;

@TargetClass(OnTimerInvokers.class)
public final class SubstituteOnTimerInvokers {

  @Substitute
  public static <InputT, OutputT> OnTimerInvoker<InputT, OutputT> forTimer(
      DoFn<InputT, OutputT> fn, String timerId) {
    throw new UnsupportedOperationException(
        "Timers are yet not supported in native mode [" + fn.getClass().getSimpleName() + "]");
  }

  @Substitute
  public static <InputT, OutputT> OnTimerInvoker<InputT, OutputT> forTimerFamily(
      DoFn<InputT, OutputT> fn, String timerFamilyId) {
    return forTimer(fn, timerFamilyId);
  }
}
