package com.ebomike.fluentasync;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static org.junit.Assert.fail;

import android.content.Context;

import androidx.test.filters.MediumTest;
import androidx.test.platform.app.InstrumentationRegistry;
import androidx.test.ext.junit.runners.AndroidJUnit4;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static java.util.stream.Collectors.toList;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Instrumented test, which will execute on an Android device.
 *
 * @see <a href="http://d.android.com/tools/testing">Testing documentation</a>
 */
@RunWith(AndroidJUnit4.class)
@MediumTest
public class FluentAsyncTest {
  private Context context;

  private static ListenableFuture<Integer> intProvider() {
    return immediateFuture(3);
  }

  private static ListenableFuture<String> convertIntToString(Integer input) {
    return immediateFuture(Integer.toString(input));
  }

  private static ListenableFuture<String> passThroughOnMainThread(String input) {
    assertThat(Thread.currentThread()).isSameInstanceAs(InstrumentationRegistry.getInstrumentation()
        .getTargetContext()
        .getMainLooper()
        .getThread());
    return immediateFuture(input);
  }

  private static ListenableFuture<String> passThroughOnWorkerThread(String input) {
    assertThat(Thread.currentThread()).isNotSameInstanceAs(
        InstrumentationRegistry.getInstrumentation()
            .getTargetContext()
            .getMainLooper()
            .getThread());
    return immediateFuture(input);
  }

  private static ListenableFuture<String> failedStringTransform(String input) {
    throw new RuntimeException("MEH");
  }

  private static ListenableFuture<Integer> failedIntProvider() {
    return immediateFailedFuture(new RuntimeException("MEH"));
  }

  @Before
  public void setUp() {
    context = InstrumentationRegistry.getInstrumentation().getTargetContext();
    FluentAsync.setErrorHandler(null);
    FluentAsync.setTaskProfiler(null);
  }

  @Test
  public void basicTransform() throws Exception {
    ListenableFuture<String> result = FluentAsync.create(context, intProvider())
        .then(FluentAsyncTest::convertIntToString)
        .run();

    assertThat(result.get()).isEqualTo("3");
  }

  @Test
  public void basicTransformBockingGet() throws Exception {
    String result = FluentAsync.create(context, intProvider())
        .then(FluentAsyncTest::convertIntToString)
        .blockingGet();

    assertThat(result).isEqualTo("3");
  }

  @Test
  public void basicTransformOnMainThread() throws Exception {
    ListenableFuture<String> result = FluentAsync.create(context, intProvider())
        .then(FluentAsyncTest::convertIntToString)
        .thenOnMainThread(FluentAsyncTest::passThroughOnMainThread)
        .run();

    assertThat(result.get()).isEqualTo("3");
  }

  @Test
  public void basicTransformOnWorkerThread() throws Exception {
    ListenableFuture<String> result = FluentAsync.create(context, intProvider())
        .then(FluentAsyncTest::convertIntToString)
        .then(FluentAsyncTest::passThroughOnWorkerThread)
        .run();

    assertThat(result.get()).isEqualTo("3");
  }

  @Test
  public void handleFailureOnTailTransform() throws Exception {
    ListenableFuture<String> result = FluentAsync.create(context, intProvider())
        .then(FluentAsyncTest::convertIntToString)
        .then(FluentAsyncTest::failedStringTransform)
        .run();

    try {
      result.get();
      // This should have thrown.
      fail();
    } catch (ExecutionException e) {
      assertThat(e).hasCauseThat().isInstanceOf(RuntimeException.class);
      assertThat(e).hasCauseThat().hasMessageThat().isEqualTo("MEH");
    }
  }

  @Test
  public void executeErrorHandler() throws Exception {
    AtomicBoolean errorHandlerSet = new AtomicBoolean();

    FluentAsync.setErrorHandler(exception -> errorHandlerSet.set(true));
    ListenableFuture<String> result = FluentAsync.create(context, intProvider())
        .then(FluentAsyncTest::convertIntToString)
        .then(FluentAsyncTest::failedStringTransform)
        .run();

    try {
      result.get();
      // This should have thrown.
      fail();
    } catch (ExecutionException e) {
      assertThat(errorHandlerSet.get()).isTrue();
    }
  }

  @Test
  public void skipErrorHandlerOnIgnoredFailures() throws Exception {
    AtomicBoolean errorHandlerSet = new AtomicBoolean();

    FluentAsync.setErrorHandler(exception -> errorHandlerSet.set(true));
    ListenableFuture<String> result = FluentAsync.create(context, intProvider())
        .then(FluentAsyncTest::convertIntToString)
        .then(FluentAsyncTest::failedStringTransform)
        .runIgnoringErrors();

    result.get();
    // Doesn't throw an error on calling get() and doesn't invoke the error handler.
    assertThat(errorHandlerSet.get()).isFalse();
  }

  @Test
  public void onErrorReturn() throws Exception {
    ListenableFuture<String> result = FluentAsync.create(context, intProvider())
        .then(FluentAsyncTest::convertIntToString)
        .then(FluentAsyncTest::failedStringTransform)
        .onErrorReturn(Exception.class, Throwable::getMessage)
        .run();

    assertThat(result.get()).isEqualTo("MEH");
  }

  @Test
  public void handleFailureOnHeadTransform() throws Exception {
    ListenableFuture<String> result = FluentAsync.create(context, failedIntProvider())
        .then(FluentAsyncTest::convertIntToString)
        .run();

    try {
      result.get();
      // This should have thrown.
      fail();
    } catch (ExecutionException e) {
      assertThat(e).hasCauseThat().isInstanceOf(RuntimeException.class);
      assertThat(e).hasCauseThat().hasMessageThat().isEqualTo("MEH");
    }
  }

  @Test
  public void ignoreFailuresOnTailTransform() throws Exception {
    ListenableFuture<String> result = FluentAsync.create(context, intProvider())
        .then(FluentAsyncTest::convertIntToString)
        .then(FluentAsyncTest::failedStringTransform)
        .runIgnoringErrors();

    // Call get() - even though the future exeution fails, we should ultimately get a result of
    // null.
    assertThat(result.get()).isNull();
  }

  @Test
  public void thenVoidOnMainThread() throws Exception {
    AtomicInteger guard = new AtomicInteger();

    ListenableFuture<Void> result = FluentAsync.create(context, intProvider())
        .then(FluentAsyncTest::convertIntToString)
        .thenVoidOnMainThread(dummy -> guard.set(3))
        .run();

    result.get();
    assertThat(guard.get()).isEqualTo(3);
  }

  @Test
  public void thenJoinMultiple() throws Exception {
    AtomicBoolean finalCheck = new AtomicBoolean();
    SettableFuture<Void> future1 = SettableFuture.create();
    SettableFuture<Void> future2 = SettableFuture.create();

    AsyncFunction<Integer, Iterable<ListenableFuture<Void>>> multipleProvider = number ->
        immediateFuture(ImmutableList.of(future1, future2));

    ListenableFuture<Void> result = FluentAsync.create(context, intProvider())
        .thenJoinMultiple(multipleProvider)
        .waitForAll()
        .thenVoid(dummy -> finalCheck.set(true))
        .run();

    assertThat(finalCheck.get()).isFalse();
    future1.set(null);
    assertThat(finalCheck.get()).isFalse();
    future2.set(null);
    result.get();
    assertThat(finalCheck.get()).isTrue();
  }

  @Test
  public void createMultiple() throws Exception {
    AtomicBoolean finalCheck = new AtomicBoolean();
    SettableFuture<Void> future1 = SettableFuture.create();
    SettableFuture<Void> future2 = SettableFuture.create();

    ListenableFuture<Iterable<ListenableFuture<Void>>> multipleFutures =
        immediateFuture(ImmutableList.of(future1, future2));

    ListenableFuture<Void> result = FluentAsync.createForMultiple(context, multipleFutures)
        .waitForAll()
        .thenVoid(dummy -> finalCheck.set(true))
        .run();

    assertThat(finalCheck.get()).isFalse();
    future1.set(null);
    assertThat(finalCheck.get()).isFalse();
    future2.set(null);
    result.get();
    assertThat(finalCheck.get()).isTrue();
  }

  @Test
  public void profile() throws Exception {
    FakeTaskProfiler profiler = new FakeTaskProfiler();
    FluentAsync.setTaskProfiler(profiler);

    ListenableFuture<String> result = FluentAsync.create(context, intProvider(), "Task1")
        .then(FluentAsyncTest::convertIntToString, "Task2")
        .then(FluentAsyncTest::passThroughOnWorkerThread, "Task3")
        .run();

    result.get();

    assertThat(profiler.getTasks().stream()
        .map(ProfiledTask::taskName).collect(toList())).containsExactly("Task1", "Task2", "Task3");
  }

  @Test
  public void profilerDisabled() throws Exception {
    ListenableFuture<String> result = FluentAsync.create(context, intProvider(), "Task1")
        .then(FluentAsyncTest::convertIntToString, "Task2")
        .then(FluentAsyncTest::passThroughOnWorkerThread, "Task3")
        .run();

    result.get();
  }

  @AutoValue
  public static abstract class ProfiledTask {
    public static ProfiledTask create(String taskName,
        float milliseconds) {
      return new AutoValue_FluentAsyncTest_ProfiledTask(taskName, milliseconds);
    }

    public abstract String taskName();

    public abstract float milliseconds();
  }

  private static class FakeTaskProfiler implements FluentAsync.TaskProfiler {
    private final ImmutableList.Builder<ProfiledTask> tasks = ImmutableList.builder();

    @Override
    public void profileTaskLength(String taskName, float milliseconds) {
      tasks.add(ProfiledTask.create(taskName, milliseconds));
    }

    ImmutableList<ProfiledTask> getTasks() {
      return tasks.build();
    }
  }
}