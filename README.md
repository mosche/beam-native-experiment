# Apache Beam native experiment

This is an experiment to run a Beam pipeline _locally_ as GraalVM native image.
Goal of the experiment is to see if it is possible to run the Beam `MinimalWordCount` example as native executable
locally and evaluate if it is a feasible approach to quickly process smaller datasets in Cloud environments in terms of
performance and resource consumption. Our expectation is too see much lower memory usage for native images, as well as
faster startup times.

For this experiment we are targeting Beam's `DirectRunner` lacking any suitable alternative local runner.
The primary focus of `DirectRunner` is to test pipelines and their components for correctness, rather than performance.
So we'll have to interpret results in that context.

While both the `SparkRunner` and `FlinkRunner` support a local, non-clustered mode, getting these frameworks to work on
native images is significantly more involved and out of scope for this initial investigation. Though, we're planning to
look into these in a follow up.

## Challenges

Java frameworks such as Apache Beam often make heavy use of reflection. Obviously that plays badly with ahead-of-time
compilation used to build native images. Another troublesome feature in this context is runtime code generation as
possible on the JVM. Both of these have to be dealt with to run Apache Beam as native image.

To offer simpler and more flexible ways of writing pipelines for users, Beam is relying heavily on **runtime code
generation** using `ByteBuddy`. Compared to an alternative reflection based approach, such generated code will
generally provide superior performance.
Despite runtime code generation being used for various features in Beam, there's just one usage we absolutely have to
address here: Beam dynamically generates an invoker class for every `DoFn`, whether provided by the user or the Beam
SDK itself. This is even the case for rather primitive `DoFns` to map, flapMap or filter data.
Beam schemas are also heavily based on runtime code generation to support efficient getters and setters for rows.
Schemas are a necessary foundation for Beam SQL. Both are considered out of scope for this experiment.

When generating code at runtime, **reflection** is typically used to gather the necessary metadata. While runtime code
generation is tackled with the point above, in Beam part of the metadata is kept as `DoFnSignature` and used at
runtime. Another, less obvious, usage of reflection is Beam's configuration mechanism, `PipelineOptions`. These
are implemented using dynamic proxies and there's a huge number of possible interfaces / combinations.

## Environment setup

To follow along, make sure you have a GraalVM installed locally. Additionally, you have to install the
[native-image tool](https://graalvm.github.io/native-build-tools/latest/gradle-plugin.html#_installing_graalvm_native_image_tool)
.

```bash
# install GraalVM using SDKman!
sdk install java 22.3.r11-grl
sdk default java 22.3.r11-grl
# install native-image tool
gu install native-image
```

## Preparing the native image

### Generate native image configuration

At first, we generate the native-image configuration from an instrumented run with the GraalVM agent. Before your first
run, you will have to download the sample data
using [gsutil](https://cloud.google.com/storage/docs/gsutil_install#install).

The agent generates configuration files that record usage of reflection, dynamic proxies and, if enabled, even supports
extracting dynamic classes generated at runtime (more on this [later](#graal-agent-class-define-support)).

```bash
gsutil cp gs://apache-beam-samples/shakespeare/kinglear.txt /tmp/data/kinglear.txt
gradle -Pagent run
gradle metadataCopy --task run --dir src/graal/resources/META-INF/native-image
```

Unfortunately, as we'll soon notice, this configuration is not sufficient yet to build an image that can be run
successfully.

#### Randomized behavior in DirectRunner

First, to prevent any potential traps with randomized behavior in `DirectRunner`, we simply patch respective code for
the purpose of this investigation. This way we don't risk comparing apples and oranges when later evaluating our
results.

Random generators can prove tricky in native images. If initialized at build time, results won't be random anymore.
Instead, each run will produce the very same sequence of numbers as the seed value was already created at build time.
Luckily, recent versions of GraalVM catch this at build time and fail compilation.

[beam-native-image](https://github.com/mosche/beam/tree/beam-native-image) contains the necessary patches for direct
runner. To keep following, pull that branch and build a local Snapshot version of the direct runner.

```bash
gradle -Ppublishing -PdistMgmtSnapshotsUrl=~/.m2/repository/ -p runners/direct-java  publishToMavenLocal -PenableCheckerFramework=false
```

Last, but not least, target parallelism of direct runner is set to 4 in the `MinimalWordCount` application itself.

### Fix usage of dynamic proxies with PipelineOptions

If attempting to run a native image compiled using the generated configuration, we'll immediately notice that the agent
did not capture all required combinations of interfaces used for `PipelineOptions`, and order matters.

Unfortunately the number of interfaces is far to large to naively generate all possible permutations.
Instead we have to make sure we create dynamic proxies from a stable, deterministic order of interfaces.

This requires a small change to Beam's `PipelineOptionsFactory` to sort the interfaces before dynamic proxies are
created:

```java
Arrays.sort(interfaces, new Comparator<Class<?>>() {
  @Override public int compare(Class<?> c1, Class<?> c2) {
    return c1.getName().compareTo(c2.getName());
  }
});
Class<T> allProxyClass = Proxy.getProxyClass(ReflectHelpers.findClassLoader(interfaces), interfaces);
```

The above mentioned branch [beam-native-image](https://github.com/mosche/beam/tree/beam-native-image) contains this fix
as well. This time we have to publish a Snapshot for Beam's core locally.

```bash
gradle -Ppublishing -PdistMgmtSnapshotsUrl=~/.m2/repository/ -p sdks/java/core  publishToMavenLocal -PenableCheckerFramework=false
```

Once done, make sure to [re-generate the image configuration](#generate-native-image-configuration) so changes are
reflected in `proxy-config.json`.

### Generate dynamic DoFn invokers at build time

Next, we want to support the generated `DoFnInvoker` classes in the native image. Luckily, these classes have a stable
deterministic name, making this task a lot easier.

#### Graal agent class define support

Unfortunately, it is not possible to simply use the
agent's [class define support](https://www.graalvm.org/22.1/reference-manual/native-image/ExperimentalAgentOptions/#support-for-predefined-classes) (
enabled using `experimental-class-define-support`) to extract the generated `DoFnInvoker` classes automatically during
an instrumented pipeline run and inject them into the native image.

Beam has to load DoFn invokers using the system classloader as well to gain (package) private access. If
loaded from a different classloader, packages would not be considered the same despite having matching names.
Unfortunately, the agent skips all classes loaded using the system classloader and hence doesn't extract the generated
invokers. See [this issue](https://github.com/oracle/graal/issues/4248) for more details.

Lacking an out of the box solution, we'll have to tackle this manually.

#### Index classpath with jandex

First, we'll need to find all `DoFns` that require an invoker. We can
use [`jandex`](https://smallrye.io/jandex/jandex/3.0.5/index.html) to index relevant parts of
the classpath and easily query for all `DoFn` implementations. This avoids having to load the entire classpath compared
to using a reflection based approach.

To index parts of the classpath, our `build.gradle` defines an intransitive configuration `jandexOnly`. Classes of jars
of this configuration, excluding their transitive dependencies, are indexed in addition to the project's sources. In our
case we're indexing Beam `core` and `direct-java`.

```groovy
jandexMain {
  inputFiles.from(
    configurations.jandexOnly
      .filter { it.name.endsWith('.jar') }
      .collect { zipTree(it).filter { it.name.endsWith('.class') } })
}
```

#### Generate invokers using native build feature (Iteration 1)

Next, we implement a native [build feature](https://build-native-java-apps.cc/developer-guide/feature/) that intercepts
image generation to generate `DoFn` invoker classes before actually analyzing what must be included in the native
image in the following build stage.

The additional `graal` source set defined for this experiment contains code that is executed during native image
generation. `PredefinedDoFnInvokerFeature` generates the invoker classes for all `DoFn` subclasses using ByteBuddy by
means of the `generateInvokerClass` utility in Beam. Note however, this required
a [tiny patch](https://github.com/mosche/beam/tree/beam-native-image) to `ByteBuddyDoFnInvokerFactory` to open up
visibility of `generateInvokerClass` so we can access the utility. That patch is already included in the snapshot of
Beam's `core` we've build above.

Despite invoker classes and respective constructors already being listed in `reflect-config.json` after
the [agent run](#generate-native-image-configuration), we have to re-register the generated classes in the feature.
Previously, when processing `reflect-config.json`, the generated invoker classes were skipped as they simply didn't
exist yet.

```java
Index index = new IndexReader(new FileInputStream(jandex)).read();
for (ClassInfo delegate : index.getAllKnownSubclasses(DoFn.class)) {
  Class<?> clazz = Class.forName(delegate.name().toString(), false, contextLoader());
  DoFnSignature signature = DoFnSignatures.getSignature((Class) clazz);

  // Dynamically generate invoker class using ByteBuddy / Beam.
  Class<?> invokerClass = generateInvokerClass(signature);

  // Register the invoker class and constructor for inclusion in the image.
  RuntimeReflection.register(invokerClass);
  RuntimeReflection.register(invokerClass.getConstructor(clazz));
}
```

Last, to be picked up, we have to register the feature as build argument in the
Gradle [native](https://graalvm.github.io/native-build-tools/latest/gradle-plugin.html#_introduction) plugin.

```groovy
graalvmNative {
  binaries {
    main {
      buildArgs '--features=beam.dofns.PredefinedDoFnInvokerFeature'
    }
  }
}
```

#### Generate invokers using native build feature (Iteration 2)

Reflecting upon the previous attempt, we've generated all possible `DoFn` signatures and invokers available on the
classpath despite being used or not. And as we're registering all of these for runtime reflecting, everything will
be included in the native image.

Luckily we can simplify the build process, so we don't even have to index the classpath anymore. Instead of
identifying `DoFn` invokers using jandex, we can extract the ones used at runtime from the agent
generated `reflect-config.json`.

And if doing so early enough in the native build before `reflect-config.json` is actually processed, we can even remove
some entries that won't be used at runtime anymore because we've done the necessary work during the build already.

### Load pre-built invokers

Having all invokers already pre-built into the image, we obviously don't want to invoke `ByteBuddy` anymore when
later calling `generateInvokerClass` at runtime. We
therefore [substitute](https://build-native-java-apps.cc/developer-guide/substitution/) respective bytecode during
compilation to just load the right invoker by name:

```java
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
```

### Class initialization

In native images classes can either be initialized at runtime or at build time.
While initialization at build time has many advantages such as shorter startup times, the semantics can be totally
different compared to initialization at runtime when first used as required by the JVM. The goal is to statically
initialize as many classes as possible while keeping semantics of the application correct.

A common pitfall used to be statically initialized random number generators as mentioned in the context of direct
runner [above](#randomized-behavior-in-directrunner). More recent versions of GraalVM will fail the build if such an
instance is detected. In our we have to force runtime initialization of Beam's `TupleTag` (and dependents) due to it's
static `Random` field (despite it being initialized with seed `0`).

### Optional: Improve DoFnSignature lookup

Finally, we can remove the code path to generate `DoFnSignature`s. All required signatures have
already been generated during image generation in the above feature and will be stored in the image.

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

In the following we're going to evaluate performance and memory usage of the native pipeline against the same pipeline
running on multiple JVMs using an uber jar. Each run is repeated 50 times on a MacBook Pro to gather sufficient data.
The `time` command is used to gather basic runtime metrics.

```bash
gradle nativeCompile
gradle shadowJar

# Configure zsh time command to emit Json for easier postprocessing
TIMEFMT='{"time_user_ms": "%mU", "time_system_ms": "%mS", "time_elapsed_ms": "%mE", "rss_max_kb": %M, "cpu_percentage": "%P"}'

# Trigger 50 instrumented pipeline runs each
repeat 50 time java -jar build/libs/beam-native-experiment-1.0-SNAPSHOT-all.jar &> /dev/null
repeat 50 time build/native/nativeCompile/beam-native-experiment &> /dev/null
```

##### Summary of results

The boxplot charts below visualize performance (time elapsed) and memory usage (max RSS) of the native image as well as
multiple JVMs based on 50 benchmark runs each. For Java 8, the G1 garbage collector was configured, which became the
default for Java 11.

Results are surprising considering our initial expectations; specifically we're looking at the first iteration here.
Memory usage only improved ~ 29% (median) compared to the best performing JVM (Java 8 using G1 GC). On the other hand,
performance also improved ~ 27% (median) compared to the best performing JVM (GraalVM CE 22.3.0). The latter was
certainly not expected as native images are not necessarily known for great performance. The JVM JIT compiler does a
great job optimizing code at runtime, which can't be done with native images. The fact that we were able to push some
costly initialization code (such as generation of `DoFn` signatures and invokers) into the native image build might
have helped there.

As expected, results of the 2nd iteration, that slightly refines the generation of `DoFn` invokers, are very similar.
The code executed at runtime is identical. The only difference is that we're not including unused `DoFn` signatures and
invokers in the native image anymore. Though, considering this, it is unexpected to see a slightly higher memory usage.

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
        "x": {"field": "runtime", "type": "nominal", "axis": {"labels": false}, "title":"Runtime system"},
        "y": {"field": "time_elapsed_ms", "type": "quantitative", "title": "Total time elapsed (ms)"},
        "color": {"field": "runtime", "type": "nominal", "scale": {"scheme": "set2"}, "title":"Runtime system"},
        "tooltip": {"field": "time_elapsed_ms", "type": "quantitative"}
      }
    },
    {
      "width": 100,
      "height": 300,
      "mark": "boxplot",
      "encoding": {
        "x": {"field": "runtime", "type": "nominal", "axis": {"labels": false}, "title":"Runtime system"},
        "y": {"field": "rss_max_kb", "type": "quantitative", "title": "Max RSS (kb)"},
        "color": {"field": "runtime", "type": "nominal"},
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
        "color": {"field": "runtime", "type": "nominal"}
      }
    }
  ]
}
```

|              | Runtime system                    | Q1        | Median    | Q3        | Max       |
|--------------|-----------------------------------|-----------|-----------|-----------|-----------|
| Time elapsed | Native - Iteration 1              | 40.8 s    | 61.8 s    | 75.1 s    | 84,3 s    |
|              | Native - Iteration 2              | 38.8 s    | 61.1 s    | 73.1 s    | 98.4 s    |
|              | JVM - Corretto 8.322 (G1)         | 106.3 s   | 118.9 s   | 130.2 s   | 153.8 s   |
|              | JVM - Corretto 11.0.16            | 88.2 s    | 102.8 s   | 113.8 s   | 166.9 s   |
|              | JVM - GraalVM CE 22.3.0 (11.0.17) | 72.8 s    | 85.2 s    | 103.2 s   | 161.2 s   |
| Max RSS      | Native - Iteration 1              | 390.8 Mb  | 473.7 Mb  | 539.9 Mb  | 752.5 s   |
|              | Native - Iteration 2              | 393.8 Mb  | 487.3 Mb  | 601.3 Mb  | 785.8 Mb  |
|              | JVM - Corretto 8.322 (G1)         | 661.1 Mb  | 670.3 Mb  | 680.7 Mb  | 692.3 Mb  |
|              | JVM - Corretto 11.0.16            | 1164.5 Mb | 1175.8 Mb | 1188.6 Mb | 1222.4 Mb |
|              | JVM - GraalVM CE 22.3.0 (11.0.17) | 1107.3 Mb | 1121.5 Mb | 1139.4 Mb | 1225.6 Mb |

### Next steps

The results seen in this investigation are promising. It is absolutely feasible to run a Beam pipeline locally as native
image. However, the unexpected high memory usage of the native pipeline - despite the improvement - is somehow a bit
disappointing. Though, this might be an issue of the local runner.

The next step here is to investigate this further with a more performance oriented runner, either using Spark or Flink
or even a new lightweight in-memory runner developed from scratch.