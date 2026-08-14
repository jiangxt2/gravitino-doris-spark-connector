/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.jiangxt2.gravitino.doris.spark;

import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.CancellationException;
import org.apache.doris.shaded.io.grpc.Status;
import org.apache.doris.shaded.io.grpc.StatusException;
import org.apache.doris.shaded.io.grpc.StatusRuntimeException;
import org.apache.doris.shaded.org.apache.arrow.adbc.core.AdbcException;
import org.apache.doris.shaded.org.apache.arrow.adbc.core.AdbcStatusCode;

/** Classifies only explicit pre-delivery transport failures as safe for Thrift fallback. */
final class DorisArrowFallbackFailureClassifier35 {

  private static final int MAX_THROWABLES = 64;

  private DorisArrowFallbackFailureClassifier35() {}

  static boolean isFallbackEligible(Throwable failure) {
    if (failure == null) {
      return false;
    }
    ArrayDeque<Throwable> pending = new ArrayDeque<>();
    Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    pending.add(failure);
    boolean transportFailure = false;
    while (!pending.isEmpty() && visited.size() < MAX_THROWABLES) {
      Throwable current = pending.removeFirst();
      if (!visited.add(current)) {
        continue;
      }
      if (isHardDenied(current)) {
        return false;
      }
      transportFailure |= isTransportFailure(current);
      if (current.getCause() != null) {
        pending.addLast(current.getCause());
      }
      for (Throwable suppressed : current.getSuppressed()) {
        if (suppressed != null) {
          pending.addLast(suppressed);
        }
      }
    }
    return pending.isEmpty() && transportFailure;
  }

  private static boolean isHardDenied(Throwable failure) {
    if (failure instanceof InterruptedException
        || failure instanceof CancellationException
        || failure instanceof Error
        || failure instanceof SecurityException) {
      return true;
    }
    if (failure instanceof AdbcException) {
      AdbcStatusCode status = ((AdbcException) failure).getStatus();
      return status != AdbcStatusCode.IO
          && status != AdbcStatusCode.TIMEOUT
          && status != AdbcStatusCode.NOT_IMPLEMENTED;
    }
    if (failure instanceof StatusRuntimeException) {
      Status.Code code = ((StatusRuntimeException) failure).getStatus().getCode();
      return code != Status.Code.UNAVAILABLE
          && code != Status.Code.DEADLINE_EXCEEDED
          && code != Status.Code.UNIMPLEMENTED;
    }
    if (failure instanceof StatusException) {
      Status.Code code = ((StatusException) failure).getStatus().getCode();
      return code != Status.Code.UNAVAILABLE
          && code != Status.Code.DEADLINE_EXCEEDED
          && code != Status.Code.UNIMPLEMENTED;
    }
    return false;
  }

  private static boolean isTransportFailure(Throwable failure) {
    if (failure instanceof ConnectException
        || failure instanceof NoRouteToHostException
        || failure instanceof SocketTimeoutException) {
      return true;
    }
    if (failure instanceof AdbcException) {
      AdbcStatusCode status = ((AdbcException) failure).getStatus();
      return status == AdbcStatusCode.IO
          || status == AdbcStatusCode.TIMEOUT
          || status == AdbcStatusCode.NOT_IMPLEMENTED;
    }
    if (failure instanceof StatusRuntimeException) {
      Status.Code code = ((StatusRuntimeException) failure).getStatus().getCode();
      return code == Status.Code.UNAVAILABLE
          || code == Status.Code.DEADLINE_EXCEEDED
          || code == Status.Code.UNIMPLEMENTED;
    }
    if (failure instanceof StatusException) {
      Status.Code code = ((StatusException) failure).getStatus().getCode();
      return code == Status.Code.UNAVAILABLE
          || code == Status.Code.DEADLINE_EXCEEDED
          || code == Status.Code.UNIMPLEMENTED;
    }
    return false;
  }
}
