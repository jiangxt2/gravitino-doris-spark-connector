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

import org.apache.gravitino.Catalog;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.JdbcCredential;
import org.apache.gravitino.credential.SupportsCredentials;

/** Resolves exactly one JDBC credential through Gravitino credential vending. */
public final class DorisCredentialResolver {

  private DorisCredentialResolver() {}

  /**
   * Resolves a JDBC credential without falling back to ordinary catalog properties.
   *
   * @param catalog the Gravitino catalog client object
   * @return the vended JDBC credential
   */
  public static JdbcCredential resolve(Catalog catalog) {
    Credential[] credentials;
    try {
      SupportsCredentials supportsCredentials = catalog.supportsCredentials();
      if (supportsCredentials == null) {
        throw missingCredential();
      }
      credentials = supportsCredentials.getCredentials();
    } catch (UnsupportedOperationException e) {
      throw missingCredential();
    }
    if (credentials == null) {
      throw missingCredential();
    }

    JdbcCredential resolved = null;
    for (Credential credential : credentials) {
      if (credential instanceof JdbcCredential) {
        if (resolved != null) {
          throw new IllegalStateException("Gravitino returned multiple JDBC credentials");
        }
        resolved = (JdbcCredential) credential;
      }
    }
    if (resolved == null) {
      throw missingCredential();
    }
    return resolved;
  }

  private static IllegalStateException missingCredential() {
    return new IllegalStateException(
        "Governed Doris reads require a vended JdbcCredential; configure "
            + "credential-providers=jdbc-user-password");
  }
}
