package ai.chronon.integrations.cloud_gcp

import com.google.api.core.ApiFuture
import com.google.api.core.ApiFutureCallback
import com.google.api.core.ApiFutures
import com.google.common.util.concurrent.MoreExecutors

import java.util.concurrent.CompletableFuture

// Fork of the bigtable-hbase ApiFutureUtils class to avoid taking a dependency on bigtable-hbase for one class
// BigTable hbase brings in a ton of dependencies that we don't need for this one class
object ApiFutureUtils {

  def toCompletableFuture[T](apiFuture: ApiFuture[T]): CompletableFuture[T] = {

    val completableFuture: CompletableFuture[T] = new CompletableFuture[T]() {

      override def cancel(mayInterruptIfRunning: Boolean): Boolean = {
        val result: Boolean = apiFuture.cancel(mayInterruptIfRunning)
        super.cancel(mayInterruptIfRunning)
        result
      }
    }

    val callback: ApiFutureCallback[T] = new ApiFutureCallback[T]() {

      override def onFailure(throwable: Throwable): Unit = {
        completableFuture.completeExceptionally(throwable)
      }

      override def onSuccess(t: T): Unit = {
        completableFuture.complete(t)
      }
    }

    ApiFutures.addCallback(apiFuture, callback, MoreExecutors.directExecutor)
    completableFuture

  }
}
