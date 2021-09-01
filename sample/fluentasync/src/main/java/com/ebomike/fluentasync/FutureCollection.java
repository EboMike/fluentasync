package com.ebomike.fluentasync;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.stream.Collectors.toList;

import android.content.Context;
import android.util.Log;

import androidx.annotation.CheckResult;
import androidx.annotation.NonNull;
import androidx.core.content.ContextCompat;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

public class FutureCollection<T> {

  private final Context context;

  private final Collection<ListenableFuture<T>> futures;

  FutureCollection(@NonNull Context context,
      @NonNull Collection<ListenableFuture<T>> futures) {
    this.context = checkNotNull(context);
    this.futures = checkNotNull(futures);
  }

  @CheckResult
  public FluentAsync<Void> waitForAll() {
    return waitForAll(directExecutor());
  }

  @CheckResult
  public FluentAsync<Void> waitForAll(@NonNull Executor executor) {
    return FluentAsync.create(context, () ->
        Futures.whenAllSucceed(futures).call(() -> null, executor));
  }

  @CheckResult
  public <O> FluentAsync<O> waitForAllThenBlocking(AsyncCollection.MultiProcessor<T, O> processor) {
    return waitForAllThenBlocking(processor, directExecutor());
  }

  @CheckResult
  public <O> FluentAsync<O> waitForAllThenBlockingOnMainThread(@NonNull Context context,
      AsyncCollection.MultiProcessor<T, O> processor) {
    return waitForAllThenBlocking(processor, ContextCompat.getMainExecutor(context));
  }

  @CheckResult
  public <O> FluentAsync<O> waitForAllThenBlocking(AsyncCollection.MultiProcessor<T, O> processor, @NonNull Executor executor) {
    return waitForAll(executor)
        .thenBlocking(dummy -> {
          List<T> results = futures.stream()
              .map(result -> {
                try {
                  return result.get();
                } catch (ExecutionException | InterruptedException e) {
                  // This shouldn't happen because we called whenAllSucceed previously.
                  Log.e("EboCore", "Failed to load already-succeeded future.", e);
                  return null;
                }
              })
              .collect(toList());

          return processor.process(results);
        });
  }

  public interface MultiProcessor<T, O> {
    O process(List<T> results);
  }
}

