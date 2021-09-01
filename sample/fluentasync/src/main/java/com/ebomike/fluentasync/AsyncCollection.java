package com.ebomike.fluentasync;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.stream.Collectors.toList;

import android.content.Context;
import android.util.Log;

import androidx.annotation.CheckResult;
import androidx.annotation.NonNull;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import javax.annotation.CheckReturnValue;

public class AsyncCollection<T> {
  private final Context context;
  private final Collection<FluentAsync<T>> asyncs;
  private final boolean usesMainThread;

  AsyncCollection(@NonNull Context context,
      @NonNull Collection<FluentAsync<T>> asyncs) {
    this.context = checkNotNull(context);
    this.asyncs = checkNotNull(asyncs);
    usesMainThread = asyncs.stream()
        .anyMatch(FluentAsync::isUsesMainThread);
  }

  @CheckResult
  @CheckReturnValue
  public FluentAsync<Void> waitForAll() {
    List<ListenableFuture<?>> futures = asyncs.stream()
        .map(FluentAsync::run)
        .collect(toList());

    return FluentAsync.create(context, () ->
            Futures.whenAllSucceed(futures).call(() -> null, directExecutor()),
        usesMainThread);
  }

  public <O> FluentAsync<O> waitForAllThenBlocking(MultiProcessor<T, O> processor) {
    return waitForAll()
        .thenBlocking(dummy -> {
          List<T> results = asyncs.stream()
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
