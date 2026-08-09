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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.gravitino.Catalog;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.JdbcCredential;
import org.apache.gravitino.credential.SupportsCredentials;
import org.junit.jupiter.api.Test;

public class TestDorisCredentialResolver {

  @Test
  void resolvesEmptyPasswordWithoutRenderingIt() {
    Catalog catalog = mock(Catalog.class);
    SupportsCredentials credentials = mock(SupportsCredentials.class);
    JdbcCredential expected = new JdbcCredential("reader", "");
    when(catalog.supportsCredentials()).thenReturn(credentials);
    when(credentials.getCredentials()).thenReturn(new Credential[] {expected});

    assertThat(DorisCredentialResolver.resolve(catalog)).isSameAs(expected);
  }

  @Test
  void failsClosedWhenCredentialIsMissingOrDuplicated() {
    Catalog catalog = mock(Catalog.class);
    assertThatThrownBy(() -> DorisCredentialResolver.resolve(catalog))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("credential-providers");

    SupportsCredentials credentials = mock(SupportsCredentials.class);
    when(catalog.supportsCredentials()).thenReturn(credentials);
    when(credentials.getCredentials()).thenReturn(new Credential[0]);
    assertThatThrownBy(() -> DorisCredentialResolver.resolve(catalog))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("credential-providers");

    when(credentials.getCredentials()).thenReturn(null);
    assertThatThrownBy(() -> DorisCredentialResolver.resolve(catalog))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("credential-providers");

    when(credentials.getCredentials())
        .thenReturn(
            new Credential[] {
              new JdbcCredential("first", "secret"), new JdbcCredential("second", "other-secret")
            });
    assertThatThrownBy(() -> DorisCredentialResolver.resolve(catalog))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageNotContaining("secret");
  }
}
