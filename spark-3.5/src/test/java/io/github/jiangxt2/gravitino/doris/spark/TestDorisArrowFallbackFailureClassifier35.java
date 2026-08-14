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

import static org.assertj.core.api.Assertions.assertThat;

import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.util.concurrent.CancellationException;
import org.apache.doris.shaded.io.grpc.Status;
import org.apache.doris.shaded.org.apache.arrow.adbc.core.AdbcException;
import org.junit.jupiter.api.Test;

public class TestDorisArrowFallbackFailureClassifier35 {

  @Test
  void acceptsOnlyTheReviewedTransportFamilies() {
    assertThat(
            DorisArrowFallbackFailureClassifier35.isFallbackEligible(
                new ConnectException("redacted")))
        .isTrue();
    assertThat(
            DorisArrowFallbackFailureClassifier35.isFallbackEligible(
                new NoRouteToHostException("redacted")))
        .isTrue();
    assertThat(
            DorisArrowFallbackFailureClassifier35.isFallbackEligible(
                new SocketTimeoutException("redacted")))
        .isTrue();
    assertThat(
            DorisArrowFallbackFailureClassifier35.isFallbackEligible(AdbcException.io("redacted")))
        .isTrue();
    assertThat(
            DorisArrowFallbackFailureClassifier35.isFallbackEligible(
                AdbcException.notImplemented("redacted")))
        .isTrue();
    assertThat(
            DorisArrowFallbackFailureClassifier35.isFallbackEligible(
                Status.UNAVAILABLE.asRuntimeException()))
        .isTrue();
    assertThat(
            DorisArrowFallbackFailureClassifier35.isFallbackEligible(
                Status.DEADLINE_EXCEEDED.asRuntimeException()))
        .isTrue();
    assertThat(
            DorisArrowFallbackFailureClassifier35.isFallbackEligible(
                Status.UNIMPLEMENTED.asException()))
        .isTrue();
  }

  @Test
  void deniesAuthenticationCancellationResourceAndUnknownFailures() {
    assertThat(
            DorisArrowFallbackFailureClassifier35.isFallbackEligible(
                Status.UNAUTHENTICATED.asRuntimeException()))
        .isFalse();
    assertThat(
            DorisArrowFallbackFailureClassifier35.isFallbackEligible(
                Status.PERMISSION_DENIED.asRuntimeException()))
        .isFalse();
    assertThat(
            DorisArrowFallbackFailureClassifier35.isFallbackEligible(
                Status.RESOURCE_EXHAUSTED.asRuntimeException()))
        .isFalse();
    assertThat(
            DorisArrowFallbackFailureClassifier35.isFallbackEligible(new CancellationException()))
        .isFalse();
    assertThat(
            DorisArrowFallbackFailureClassifier35.isFallbackEligible(
                new IllegalArgumentException("redacted")))
        .isFalse();
    AssertionError error = new AssertionError("redacted");
    error.addSuppressed(new ConnectException("redacted"));
    assertThat(DorisArrowFallbackFailureClassifier35.isFallbackEligible(error)).isFalse();
  }

  @Test
  void traversesCauseAndSuppressedGraphsWithHardDenyPrecedenceAndCycles() {
    RuntimeException wrapper = new RuntimeException("redacted", new ConnectException("redacted"));
    assertThat(DorisArrowFallbackFailureClassifier35.isFallbackEligible(wrapper)).isTrue();

    wrapper.addSuppressed(Status.UNAUTHENTICATED.asRuntimeException());
    assertThat(DorisArrowFallbackFailureClassifier35.isFallbackEligible(wrapper)).isFalse();

    RuntimeException first = new RuntimeException("first");
    RuntimeException second = new RuntimeException("second", new ConnectException("redacted"));
    first.addSuppressed(second);
    second.addSuppressed(first);
    assertThat(DorisArrowFallbackFailureClassifier35.isFallbackEligible(first)).isTrue();

    RuntimeException deep = new RuntimeException("root");
    Throwable cursor = deep;
    for (int index = 0; index < 64; index++) {
      RuntimeException next = new RuntimeException("nested");
      cursor.addSuppressed(next);
      cursor = next;
    }
    cursor.addSuppressed(new ConnectException("redacted"));
    assertThat(DorisArrowFallbackFailureClassifier35.isFallbackEligible(deep)).isFalse();
  }
}
