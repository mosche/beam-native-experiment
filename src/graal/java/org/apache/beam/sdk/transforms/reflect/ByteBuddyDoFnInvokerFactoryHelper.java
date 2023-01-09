package org.apache.beam.sdk.transforms.reflect;

public class ByteBuddyDoFnInvokerFactoryHelper {
  public static Class<?> generateInvokerClass(DoFnSignature signature){
    return ByteBuddyDoFnInvokerFactory.generateInvokerClass(signature);
  }
}
