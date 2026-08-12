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

package io.github.jiangxt2.gravitino.doris.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Focused validation of the generated PKCS#12 TLS fixtures without starting Docker. */
public class TlsTestCertificatesIT {

  @Test
  void generatesReadableTrustedExpiredAndUnknownCaFixtures(
      @TempDir(cleanup = org.junit.jupiter.api.io.CleanupMode.ALWAYS) Path temporaryDirectory)
      throws Exception {
    TlsTestCertificates certificates = TlsTestCertificates.prepare(temporaryDirectory);
    certificates.generate();

    assertThat(PosixFilePermissions.toString(Files.getPosixFilePermissions(temporaryDirectory)))
        .isEqualTo("rwxr-xr-x");
    assertThat(
            PosixFilePermissions.toString(
                Files.getPosixFilePermissions(
                    temporaryDirectory.resolve("generation/ca-keys.p12"))))
        .isEqualTo("rw-------");
    assertThat(certificates.clientTrustStore()).isRegularFile();

    KeyStore trustStore = load(certificates.clientTrustStore(), new char[0], "JKS");
    assertThat(trustStore.containsAlias("governed-doris-it-ca")).isTrue();
    KeyStore connectorJTrustStore = load(certificates.clientTrustStore(), null, "JKS");
    assertThat(connectorJTrustStore.containsAlias("governed-doris-it-ca")).isTrue();

    KeyStore validStore =
        load(
            certificates.feDirectory().resolve("server_certificate.p12"),
            TlsTestCertificates.FE_STORE_PASSWORD.toCharArray(),
            "PKCS12");
    X509Certificate valid = (X509Certificate) validStore.getCertificate("server");
    valid.checkValidity();
    assertThat(valid.getSubjectAlternativeNames().toString())
        .contains("10.20.30.1")
        .contains("doris-fe")
        .doesNotContain("doris-fe-mismatch");

    KeyStore expiredStore =
        load(
            certificates.feDirectory().resolve("expired-server-certificate.p12"),
            TlsTestCertificates.FE_STORE_PASSWORD.toCharArray(),
            "PKCS12");
    X509Certificate expired = (X509Certificate) expiredStore.getCertificate("server");
    assertThatThrownBy(expired::checkValidity)
        .isInstanceOf(java.security.cert.CertificateExpiredException.class);

    KeyStore unknownStore =
        load(
            certificates.feDirectory().resolve("unknown-ca-server-certificate.p12"),
            TlsTestCertificates.FE_STORE_PASSWORD.toCharArray(),
            "PKCS12");
    X509Certificate unknown = (X509Certificate) unknownStore.getCertificate("server");
    assertThat(unknown.getIssuerX500Principal()).isEqualTo(unknown.getSubjectX500Principal());
  }

  private static KeyStore load(Path path, char[] password, String storeType) throws Exception {
    KeyStore store = KeyStore.getInstance(storeType);
    try (InputStream input = Files.newInputStream(path)) {
      store.load(input, password);
    }
    return store;
  }
}
