# Apache Beam native experiment

This is an experiment to run a Beam pipeline _locally_ as GraalVM native image.
Goal of the experiment is to run the Beam `MinimalWordCount` example as native image locally
using `DirectRunner`.

Some of the major challenges expected for this experiment are Beam's usage of

- proxies for `PipelineOptions` and
- dynamically generated classes such as the `DoFn` invokers.

Unfortunately, it is not possible to simply use the
agent's [class define support](https://www.graalvm.org/22.1/reference-manual/native-image/ExperimentalAgentOptions/#support-for-predefined-classes) (
enabled using `experimental-class-define-support`) to extract such classes during a instrumented
pipeline run. Beam loads such classes into the context classloader to support private access, but
class define support will [skip such classes](https://github.com/oracle/graal/issues/4248).

## Environment setup

Make sure you have a GraalVM installed locally. Additionally, you have to install the
[native-image tool](https://graalvm.github.io/native-build-tools/latest/gradle-plugin.html#_installing_graalvm_native_image_tool)
.

```bash
# install GraalVM using SDKman!
sdk install java 22.3.r11-grl
sdk default java 22.3.r11-grl
# install native-image tool
gu install native-image
```

## Generating the image configuration

At first, we generate the native-image configuration from an instrumented run with the GraalVM
agent. Before your first run, you will have to download the sample data
using [gsutil](https://cloud.google.com/storage/docs/gsutil_install#install).

```bash
gsutil cp gs://apache-beam-samples/shakespeare/kinglear.txt /tmp/data/kinglear.txt
gradle -Pagent run
gradle metadataCopy --task run --dir src/main/resources/META-INF/native-image
```

## Building the native image

### Patching Beam

Addressing the challenges mentioned above, let's first fix Beam so we can use dynamic proxies
for `PipelineOptions` in the native image. What's key here is to create proxies from a stable,
deterministic order of interfaces. This is necessary because each such order has to be registered
when building the image. If we cannot rely on a deterministic order, the number of possible
permutations we would have to register quickly explodes.

```java
Arrays.sort(interfaces, new Comparator<Class<?>>() {
  @Override public int compare(Class<?> c1, Class<?> c2) {
    return c1.getName().compareTo(c2.getName());
  }
});
Class<T> allProxyClass = Proxy.getProxyClass(ReflectHelpers.findClassLoader(interfaces), interfaces);
```

Next, we have to generate the dynamically generated `DoFnInvokers` at image build time.
Unfortunately, this requires
another [small patch](https://github.com/apache/beam/compare/v2.43.0...mosche:beam:beam-native-image#diff-9cec8ec374edf5ad472d05effe1f9fdef8e900ac0c31246ce2f160263d6e6779)
of Beam, so we can hook into the respective code in `ByteBuddyDoFnInvokerFactory` and store the
generated classes on the image classpath.

#### Randomized behavior in DirectRunner

Finally, to prevent any potential traps with randomized behavior in `DirectRunner`, we simply remove
respective parts for the purpose of this investigation.
Random generators can prove tricky in native images. If initialized at build time, results won't be
random anymore. Instead, each run will produce the very same sequence of numbers as we're using the
exactly same seed value each time.

With these few these patches in place, you can build a Snapshot version of Beam that can run on a
native image. To do so, pull this [branch](https://github.com/mosche/beam/tree/beam-native-image)
and publish it locally.

```bash
gradle -Ppublishing -PdistMgmtSnapshotsUrl=~/.m2/repository/ -p sdks/java/core  publishToMavenLocal -PenableCheckerFramework=false
gradle -Ppublishing -PdistMgmtSnapshotsUrl=~/.m2/repository/ -p runners/direct-java  publishToMavenLocal -PenableCheckerFramework=false
```

### Generate dynamic classes during build

Our next challenge is to find all `DoFns` that require an invoker.

We can use [`jandex`](https://smallrye.io/jandex/jandex/3.0.5/index.html) to index relevant parts of
the classpath and query for all `DoFn` implementations.

`build.gradle` defines an intransitive configuration `jandexOnly`. The content of jars of this
configuration without any transitive dependencies, in our case just Beam `core` and `direct-java`,
is indexed in addition to the main sources:

```groovy
jandexMain {
  inputFiles.from(
    configurations.jandexOnly
      .filter { it.name.endsWith('.jar') }
      .collect { zipTree(it).filter { it.name.endsWith('.class') } })
}
```

The `PredefinedDoFnInvokerFeature` implements
a [build feature](https://build-native-java-apps.cc/developer-guide/feature/) that generates
invokers for all `DoFn`subclasses and registers them for reflective instantiation before doing any
code analysis. `ByteBuddyDoFnInvokerFactory.generateInvokerType` is the hook we need to access and
write the generated classes.

```java
Index index = new IndexReader(new FileInputStream(jandex)).read();
for (ClassInfo delegate : index.getAllKnownSubclasses(DoFn.class)) {
  Class<?> clazz = Class.forName(delegate.name().toString(), false, contextLoader());
  DoFnSignature signature = DoFnSignatures.getSignature((Class) clazz);

  Loaded<?> type = ByteBuddyDoFnInvokerFactory.generateInvokerType(signature);
  type.saveIn(classesDir);

  Class<?> invokerClass = type.getLoaded();
  RuntimeReflection.register(invokerClass);
  RuntimeReflection.register(invokerClass.getConstructor(clazz));
}
```

To be picked up, we have to register the feature as build argument in the
Gradle [native](https://graalvm.github.io/native-build-tools/latest/gradle-plugin.html#_introduction)
plugin.

```groovy
graalvmNative {
  binaries {
    main {
      buildArgs '--features=org.apache.beam.sdk.transforms.reflect.PredefinedDoFnInvokerFeature'
    }
  }
}
```

### Loading pre-built classes

Having all invokers pre-built, we obviously don't want to invoke `ByteBuddy` anymore when calling
`ByteBuddyDoFnInvokerFactory.generateInvokerClass(DoFnSignature signature)`. We can substitute
respective bytecode during compilation to just load the right invoker by name:

```java
@TargetClass(ByteBuddyDoFnInvokerFactory.class)
public final class SubstituteByteBuddyDoFnInvokerFactory {

  @Substitute
  private static Class<? extends DoFnInvoker<?, ?>> generateInvokerClass(DoFnSignature signature) {
    String invokerName =
        StableInvokerNamingStrategy.forDoFnClass(signature.fnClass())
            .withSuffix(DoFnInvoker.class.getSimpleName())
            .name(new TypeDescription.ForLoadedType(DoFnInvoker.class));
    try {
      return (Class) Class.forName(invokerName);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}
```

### Optional: Improve DoFnSignature lookup

Finally, we can remove the code path to generate `DoFnSignature`s. All required signatures have
already been generated during compilation in the above feature and will be stored in the image.
As `signatureCache` has become read-only, we can also replace the `ConcurrentHashMap` with a
standard `HashMap`. Though, that's more to showcase the possibilities, rather than fixing a real
performance bottleneck.

```java
@TargetClass(DoFnSignatures.class)
public final class SubstituteDoFnSignatures {

  @Alias
  @RecomputeFieldValue(kind = Kind.Custom, declClass = MapTransformer.class, isFinal = true)
  private static Map<Class<? extends DoFn<?, ?>>, DoFnSignature> signatureCache;

  public static class MapTransformer implements FieldValueTransformer {
    @Override
    public Object transform(Object receiver, Object originalValue) {
      return new HashMap<>((Map) originalValue);
    }
  }

  @Substitute
  public static <FnT extends DoFn<?, ?>> DoFnSignature getSignature(Class<FnT> fn) {
    return signatureCache.get(fn); // don't call parseSignature if absent
  }

  @Delete
  private static DoFnSignature parseSignature(Class<? extends DoFn<?, ?>> fnClass) {
    return null;
  }
}
```

### Evaluation

In the following we're going to evaluate performance and memory usage of the native pipeline against
the same pipeline running on the JVM. To do so, we build both the native-image and an uber jar.
The `time` command allows to gather some basic runtime metrics.

```bash
gradle nativeCompile
gradle shadowJar
```

```bash
# Configure zsh time command to emit Json for easier postprocessing
TIMEFMT='{"time_user_ms": "%mU", "time_system_ms": "%mS", "time_elapsed_ms": "%mE", "rss_max_kb": %M, "cpu_percentage": "%P"}'

# Trigger 50 instrumented pipeline runs each
repeat 50 time java -jar build/libs/beam-native-experiment-1.0-SNAPSHOT-all.jar &> /dev/null
repeat 50 time build/native/nativeCompile/beam-native-experiment &> /dev/null
```

The boxplot charts below visualize performance (time elapsed) and memory usage (max RSS) based on 50
benchmark runs each. We can see performance improved by roughly 10% (median) using the native-image.
For memory usage the improvement is about 55% (median). That's a lot more significant, but still not
as much as expected.

```vega-lite
{
  "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
  "data": {"url": "results.json"},
  "spacing": 100,
  "hconcat": [
    {
      "width": 100,
      "height": 300,
      "mark": "boxplot",
      "encoding": {
        "x": {"field": "type", "type": "nominal", "axis": {"labels": false}},
        "y": {"field": "time_elapsed_ms", "type": "quantitative", "title": "Total time elapsed (ms)"},
        "color": {"field": "type", "type": "nominal", "scale": {"scheme": "set2"}},
        "tooltip": {"field": "time_elapsed_ms", "type": "quantitative"}
      }
    },
    {
      "width": 100,
      "height": 300,
      "mark": "boxplot",
      "encoding": {
        "x": {"field": "type", "type": "nominal", "axis": {"labels": false}},
        "y": {"field": "rss_max_kb", "type": "quantitative", "title": "Max RSS (kb)"},
        "color": {"field": "type", "type": "nominal"},
        "tooltip": {"field": "rss_max_kb", "type": "quantitative"}
      }
    },
    {
      "width": 300,
      "height": 300,
      "mark": "point",
      "encoding": {
        "x": {"field": "time_elapsed_ms", "type": "quantitative", "title": "Total time elapsed (ms)"},
        "y": {"field": "rss_max_kb", "type": "quantitative", "title": "Max RSS (kb)"},
        "color": {"field": "type", "type": "nominal"}
      }
    }
  ]
}
```

##### Summary of  data visualized above

|              | Type       | Q1        | Median    | Q3        |
|--------------|------------|-----------|-----------|-----------|
| Time elapsed | **Native** | 36.2 s    | 53.0 s    | 58.8 s    |
|              | **JVM**    | 48.7 s    | 58.7 s    | 74.2 s    |
| Max RSS      | **Native** | 383.8 Mb  | 480.4 Mb  | 556.1 Mb  |
|              | **JVM**    | 1038.9 Mb | 1059.3 Mb | 1077.6 Mb |

## Appendix

### Flink runner

The Flink runner was another candidate for this investigation, due to the nature of Flink with a lot
more trouble expected. Flink system loader mechanism for Akka requires additional efforts to work in
native mode, but was surprisingly easy to solve:

1. Extract `flink-rpc-akka.jar` from `flink-rpc-akka-loader-x.y.z.jar`.

2. Substitute `AkkaRpcSystemLoader` to load `AkkaRpcSystem` directly from classpath.
    ```java
    @TargetClass(AkkaRpcSystemLoader.class)
    public final class SubstituteAkkaRpcSystemLoader {
    
      @Substitute
      public RpcSystem loadRpcSystem(Configuration config) {
        return new AkkaRpcSystem();
      }
    }
    ```

However, similarly to Beam, Flink is using dynamic proxies without any deterministic order of
interfaces. Proceeding with the experiment on Flink would require a patched version to work around
the [interface problem](#patching-beam) presented earlier.