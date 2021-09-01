package com.ebomike.fluentasync;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Futures.catching;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

import android.content.Context;
import android.os.Looper;
import android.util.Log;

import androidx.annotation.CheckResult;
import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.core.content.ContextCompat;
import androidx.core.util.Consumer;

import com.google.common.base.Function;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import javax.annotation.CheckReturnValue;

/**
 * Convenience handler to combine multiple asynchronous functions that return ListenableFutures.
 * <p>
 * Example:
 * <p>
 * <code>
 * ListenableFuture<T> future = FluentAsync<>.create(context, future)
 * .then(futureProvider)
 * .thenOnMainThread(futureProvider)
 * .run();
 * </code>
 * If an exception is thrown anywhere along the chain, it will be logged and passed down.
 */
public class FluentAsync<T> {
  @Nullable
  private static ErrorHandler errorHandler;
  @Nullable
  private static TaskProfiler taskProfiler;
  /**
   * Context, necessary to get the main thread.
   */
  private final Context context;
  /**
   * This is the future in the chain that performs the entire transformation.
   */
  private final ListenableFuture<T> headFuture;
  /**
   * True if any part of this chain is using the main thread.
   */
  private final boolean usesMainThread;
  /** Task profiling tracker, if enabled. */
  @Nullable
  private final TaskTracker taskTracker;
  private FluentAsync(@NonNull Context context, @NonNull ListenableFuture<T> future,
      boolean usesMainThread, @Nullable String profileTaskName) {
    this.context = checkNotNull(context);
    headFuture = checkNotNull(future);
    this.usesMainThread = usesMainThread;
    this.taskTracker = createTaskTracker(profileTaskName);
  }

  /**
   * Creates a new instance of an FluentAsync chain. Must be terminated with a call to {@link #run}.
   */
  public static <T> FluentAsync<T> create(@NonNull Context context,
      @NonNull ListenableFuture<T> future) {
    return create(context, future, /* usesMainThread= */ false);
  }

  public static <T> FluentAsync<T> create(@NonNull Context context,
      @NonNull ListenableFuture<T> future, String profileTaskName) {
    return create(context, future, /* usesMainThread= */ false, profileTaskName);
  }

  public static <T> FluentAsync<T> create(@NonNull Context context,
      @NonNull ListenableFuture<T> future, boolean usesMainThread) {
    return create(context, future, usesMainThread, /* profileTaskName= */ null);
  }

  public static <T> FluentAsync<T> create(@NonNull Context context,
      @NonNull ListenableFuture<T> future, boolean usesMainThread,
      @Nullable String profileTaskName) {
    return new FluentAsync<>(context, future, usesMainThread, profileTaskName);
  }

  public static <T> FluentAsync<T> create(@NonNull Context context,
      @NonNull FutureProvider<T> future) {
    return create(context, future.provideFuture(), /* usesMainThread= */ false);
  }

  public static <T> FluentAsync<T> create(@NonNull Context context,
      @NonNull FluentAsync<T> future) {
    return create(context, future.run(), future.isUsesMainThread());
  }

  public static <T> FluentAsync<T> create(@NonNull Context context,
      @NonNull FutureProvider<T> future, boolean usesMainThread) {
    return new FluentAsync<>(context, future.provideFuture(), usesMainThread, null);
  }

  @Deprecated
  // TODO: Just do a rename and then delete it.
  public static <T> FluentAsync<T> createImmediate(@NonNull Context context,
      @Nullable T immediateResult) {
    return create(context, immediateFuture(immediateResult), /* usesMainThread= */ false);
  }

  public static <T> FluentAsync<T> immediateAsync(@NonNull Context context,
      @Nullable T immediateResult) {
    return create(context, immediateFuture(immediateResult), /* usesMainThread= */ false);
  }

  public static FluentAsync<Void> immediateVoidAsync(@NonNull Context context) {
    return create(context, immediateFuture(null), /* usesMainThread= */ false);
  }

  /**
   * Creates a new instance of an FluentAsync chain that starts by taking multiple futures. Must be
   * terminated with a call to {@link #run}.
   **/
  public static <T> AsyncMultiple<T> createForMultiple(@NonNull Context context,
      @NonNull ListenableFuture<Iterable<ListenableFuture<T>>> futures) {
    return new AsyncMultiple<>(context, futures);
  }

  public static <T> AsyncCollection<T> createForMultiple(@NonNull Context context,
      @NonNull Collection<FluentAsync<T>> asyncs) {
    return new AsyncCollection<>(context, asyncs);
  }

  public static <T> FutureCollection<T> createForMultipleFutures(@NonNull Context context,
      @NonNull Collection<ListenableFuture<T>> futures) {
    return new FutureCollection<>(context, futures);
  }

  /**
   * Set a global error handler for unhandled exceptions in futures.
   */
  public static void setErrorHandler(@Nullable ErrorHandler errorHandler) {
    FluentAsync.errorHandler = errorHandler;
  }

  /** Sets a global task profiler. */
  public static void setTaskProfiler(@Nullable TaskProfiler taskProfiler) {
    FluentAsync.taskProfiler = taskProfiler;
  }

  @Nullable
  private static TaskTracker createTaskTracker(@Nullable String profileTaskName) {
    if (profileTaskName == null || taskProfiler == null) {
      return null;
    }

    return new TaskTracker(profileTaskName);
  }

  /**
   * Adds another async code block that takes the input from the previous one. This is called on the
   * same thread as the previous one, so presumably on a worker thread.
   */
  @CheckReturnValue
  public <O> FluentAsync<O> then(AsyncFunction<T, O> futureProvider) {
    return then(futureProvider, null);
  }

  @CheckReturnValue
  public <O> FluentAsync<O> then(AsyncFunction<T, O> futureProvider, String taskProfileName) {
    return new FluentAsync<>(context, runTransform(futureProvider, directExecutor()), usesMainThread,
        taskProfileName);
  }

  @CheckReturnValue
  public <O> FluentAsync<O> thenAsync(AsyncProvider<T, O> asyncProvider) {
    return new FluentAsync<>(context, runTransform(asyncProvider, directExecutor()), usesMainThread,
        null);
  }

  /**
   * Adds another code block that takes the input from the previous one and doesn't return
   * anything. This is called on the same thread as the previous one, so presumably on a worker
   * thread.
   */
  @CheckReturnValue
  public FluentAsync<Void> thenVoid(Consumer<T> processor) {
    return new FluentAsync<>(context, transform(headFuture, argument -> {
      processor.accept(argument);
      return null;
    }, directExecutor()), usesMainThread, null);
  }

  /**
   * Adds another code block that takes the input from the previous one and runs without returning
   * future. This is called on the same thread as the previous one, so presumably on a worker
   * thread.
   */
  @CheckReturnValue
  public <O> FluentAsync<O> thenBlocking(Function<T, O> futureProvider) {
    return new FluentAsync<>(context,
        transform(headFuture, futureProvider, directExecutor()),
        usesMainThread, null);
  }

  /**
   * Adds another code block that takes the input from the previous one and runs without returning
   * future. This is called on the main thread.
   */
  @CheckReturnValue
  public <O> FluentAsync<O> thenBlockingOnMainThread(Function<T, O> futureProvider) {
    return new FluentAsync<>(context, transform(headFuture, futureProvider,
        ContextCompat.getMainExecutor(context)), /* usesMainThread= */ true, null);
  }

  @CheckReturnValue
  public AsyncMultiple<Void> thenJoinMultiple(AsyncFunction<T,
      Iterable<ListenableFuture<Void>>> futuresProvider) {
    return new AsyncMultiple<>(context, transformAsync(headFuture, futuresProvider,
        directExecutor()));
  }

  @CheckReturnValue
  public AsyncMultiple<Void> thenJoinMultipleOnMainThread(AsyncFunction<T,
      Iterable<ListenableFuture<Void>>> futuresProvider) {
    return new AsyncMultiple<>(context, transformAsync(headFuture, futuresProvider,
        ContextCompat.getMainExecutor(context)));
  }

  /**
   * Adds another code block that takes the input from the previous one and doesn't return
   * anything. This is called on the same thread as the previous one, so presumably on a worker
   * thread.
   */
  @CheckReturnValue
  public FluentAsync<Void> thenVoidOnMainThread(Consumer<T> processor) {
    return new FluentAsync<>(context, transform(headFuture, argument -> {
      processor.accept(argument);
      return null;
    }, ContextCompat.getMainExecutor(context)), /* usesMainThread= */ true, null);
  }

  /**
   * Adds another async code block that takes the input from the previous one. This will always be
   * called on the main thread.
   */
  @CheckReturnValue
  public <O> FluentAsync<O> thenOnMainThread(AsyncFunction<T, O> futureProvider) {
    return new FluentAsync<>(context, runTransform(futureProvider,
        ContextCompat.getMainExecutor(context)), /* usesMainThread= */ true, null);
  }

  @CheckReturnValue
  public <O> FluentAsync<O> thenAsyncOnMainThread(AsyncProvider<T, O> asyncProvider) {
    return new FluentAsync<>(context, transformAsync(headFuture,
        result -> asyncProvider.provideAsync(result).run(),
        ContextCompat.getMainExecutor(context)), /* usesMainThread= */ true, null);
  }

  /**
   * Adds an error handler that will catch any exception of the given type that may have occurred
   * up until that point and returns a provided value instead.
   * <p>
   * The value provider will be called on the same thread as the future.
   */
  @CheckReturnValue
  public <E extends Exception> FluentAsync<T> onErrorReturn(Class<E> exceptionClass,
      Function<E, T> valueProvider) {
    return new FluentAsync<>(context, catching(headFuture, exceptionClass, valueProvider,
        directExecutor()), usesMainThread, null);
  }

  /**
   * Instructs the async chain to ignore any error that happened up to this point and simply return
   * null if one occurred.
   */
  @CheckReturnValue
  public FluentAsync<T> ignoreAllErrors() {
    return new FluentAsync<>(context, catching(headFuture, Exception.class, exception -> {
          Log.w("EboCore", "Ignoring async error", exception);
          return null;
        },
        directExecutor()), usesMainThread, null);
  }

  /**
   * Terminates the async chain and returns the future that will return the final result of the
   * chain, or null if an error that occurred on the way. The future will always be successful.
   */
  @CheckResult
  public ListenableFuture<T> runIgnoringErrors() {
    return catching(headFuture, Exception.class, exception -> {
      Log.e("EboCore", "Error running chained future", exception);
      return null;
    }, directExecutor());
  }

  /**
   * Terminates the async chain and returns the future that will return the final result of the
   * chain, or the error that occurred on the way (in which case the future will fail with an
   * exception).
   * <p>
   * Also adds an error handler to log an exception if one occurs.
   */
  @CheckResult
  public ListenableFuture<T> run() {
    // Add an error handler. Note that we ignore the result of the catching() call. That way, we
    // will log the error, but the actual exception returned to the caller will still fail.
    catching(headFuture, Exception.class, exception -> {
      Log.e("EboCore", "Error running chained future", exception);

      if (errorHandler != null) {
        errorHandler.onException(exception);
      }

      // Rethrow the exception. We need to wrap it in an unchecked exception.
      throw new RuntimeException(exception);
    }, directExecutor());

    return transform(headFuture, result -> {
      profileTask();
      return result;
    }, directExecutor());
  }

  /**
   * Blocks the current thread until the entire chain is complete.
   * <p>
   * This should be rarely used, especially on the main thread, but there may be a few legitimate
   * use cases, like populatig a context menu.
   * <p>
   * Note that you may not call this function from the main thread if any part of this chain is on
   * the main thread. Since this function would block the main thread, any segment that is scheduled
   * to run on the main thread would never get executed and the execution would hang indefinitely.
   * <p>
   * If any part of the chain throws an exception, this function throws an ExecutionException.
   */
  public T blockingGet() throws ExecutionException, InterruptedException {
    if (usesMainThread && Thread.currentThread() == Looper.getMainLooper().getThread()) {
      IllegalStateException exception = new IllegalStateException(
          "Blocking get on main thread would deadlock");
      if (errorHandler != null) {
        errorHandler.onException(exception);
      }

      throw exception;
    }

    return headFuture.get();
  }

  public T get() throws ExecutionException, InterruptedException {
    if (!headFuture.isDone()) {
      throw new IllegalStateException("Future still pending, use blockingGet() to block");
    }

    return headFuture.get();
  }

  public boolean isUsesMainThread() {
    return usesMainThread;
  }

  private <O> ListenableFuture<O> runTransform(AsyncFunction<T, O> futureProvider,
      Executor executor) {
    return transformAsync(headFuture, profileAndTransform(futureProvider), executor);
  }

  private <O> ListenableFuture<O> runTransform(AsyncProvider<T, O> asyncProvider,
      Executor executor) {
    return transformAsync(headFuture,
        profileAndTransform(input -> asyncProvider.provideAsync(input).run()), executor);
  }

  private <O> AsyncFunction<T, O> profileAndTransform(AsyncFunction<T, O> transformer) {
    return input -> {
      profileTask();
      return transformer.apply(input);
    };
  }

  private void profileTask() {
    if (taskTracker != null && taskProfiler != null) {
      taskProfiler.profileTaskLength(taskTracker.getTaskName(),
          taskTracker.getElapsedMillis());
    }
  }

  public interface FutureProvider<T> {
    ListenableFuture<T> provideFuture();
  }

  public interface AsyncProvider<I, T> {
    FluentAsync<T> provideAsync(I input);
  }

  /**
   * Error handler that is called when a future failed and wasn't told to ignore errors.
   */
  public interface ErrorHandler {
    void onException(Exception error);
  }

  /** Task profiler to store metrics for durations. */
  public interface TaskProfiler {
    void profileTaskLength(String taskName, float milliseconds);
  }

  /** Tracker for profiling purposes to measure the duration of an async task. */
  private static class TaskTracker {
    /** Name of the task, used for display and tagging. */
    private final String taskName;

    private final Stopwatch stopwatch;

    TaskTracker(String taskName) {
      this.taskName = checkNotNull(taskName);
      this.stopwatch = Stopwatch.createStarted();
    }

    String getTaskName() {
      return taskName;
    }

    float getElapsedMillis() {
      return stopwatch.elapsed(TimeUnit.MILLISECONDS);
    }
  }

  public static class AsyncMultiple<T> {
    private final Context context;

    private final ListenableFuture<Iterable<ListenableFuture<T>>> futures;

    AsyncMultiple(@NonNull Context context,
        @NonNull ListenableFuture<Iterable<ListenableFuture<T>>> futures) {
      this.context = checkNotNull(context);
      this.futures = checkNotNull(futures);
    }

    @CheckResult
    public FluentAsync<Void> waitForAll() {
      return FluentAsync.create(context,
          transformAsync(futures, futureList -> Futures.whenAllSucceed(futureList).call(() -> null,
              directExecutor()), directExecutor()));
    }
  }
}
