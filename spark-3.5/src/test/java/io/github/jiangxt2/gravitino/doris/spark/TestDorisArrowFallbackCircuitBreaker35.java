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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class TestDorisArrowFallbackCircuitBreaker35 {

  @AfterEach
  void clearCircuit() {
    DorisArrowFallbackCircuitBreaker35.clearForTests();
  }

  @Test
  void staysOpenForOneApplicationAndEndpointButNotAnotherApplication() {
    DorisArrowFallbackCircuitBreaker35.open("app-1", "endpoint-a");
    assertThat(DorisArrowFallbackCircuitBreaker35.isOpen("app-1", "endpoint-a")).isTrue();
    assertThat(DorisArrowFallbackCircuitBreaker35.isOpen("app-1", "endpoint-b")).isFalse();
    assertThat(DorisArrowFallbackCircuitBreaker35.isOpen("app-2", "endpoint-a")).isFalse();

    // There is deliberately no reset or time-based transition for app-1.
    assertThat(DorisArrowFallbackCircuitBreaker35.isOpen("app-1", "endpoint-a")).isTrue();
  }

  @Test
  void concurrentOpenIsAtomicAndIdempotent() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(8);
    try {
      List<Callable<Void>> opens = new ArrayList<>();
      for (int index = 0; index < 64; index++) {
        opens.add(
            () -> {
              DorisArrowFallbackCircuitBreaker35.open("app", "endpoint");
              return null;
            });
      }
      executor.invokeAll(opens);
      assertThat(DorisArrowFallbackCircuitBreaker35.isOpen("app", "endpoint")).isTrue();
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void recordsConcurrentAttemptsAtomicallyForOneApplicationAndEndpoint() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(8);
    try {
      List<Callable<Void>> attempts = new ArrayList<>();
      for (int index = 0; index < 64; index++) {
        attempts.add(
            () -> {
              DorisArrowFallbackCircuitBreaker35.recordAttempt("app", "endpoint");
              return null;
            });
      }
      executor.invokeAll(attempts);
      assertThat(DorisArrowFallbackCircuitBreaker35.attemptCount("app", "endpoint")).isEqualTo(64L);
      assertThat(DorisArrowFallbackCircuitBreaker35.attemptCount("other-app", "endpoint")).isZero();
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void storesOnlyHashedCircuitKeysInJvmGlobalProperties() {
    DorisArrowFallbackCircuitBreaker35.open("sensitive-application", "sensitive-endpoint");
    DorisArrowFallbackCircuitBreaker35.recordAttempt("sensitive-application", "sensitive-endpoint");

    assertThat(
            DorisArrowFallbackCircuitBreaker35.isOpen(
                "sensitive-application", "sensitive-endpoint"))
        .isTrue();
    assertThat(System.getProperties().stringPropertyNames())
        .noneMatch(
            property ->
                property.contains("sensitive-application")
                    || property.contains("sensitive-endpoint"));
  }
}
