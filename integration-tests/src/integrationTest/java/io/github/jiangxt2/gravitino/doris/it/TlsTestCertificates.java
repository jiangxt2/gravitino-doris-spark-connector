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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.util.ArrayList;
import java.util.List;

/** JDK-generated private-CA fixtures for the verified JDBC transport integration tests. */
final class TlsTestCertificates {

  static final String FE_STORE_PASSWORD = "doris-it";
  private static final String STORE_TYPE = "PKCS12";

  private final Path root;
  private final Path generationDirectory;
  private final Path feDirectory;
  private final Path clientDirectory;

  private TlsTestCertificates(
      Path root, Path generationDirectory, Path feDirectory, Path clientDirectory) {
    this.root = root;
    this.generationDirectory = generationDirectory;
    this.feDirectory = feDirectory;
    this.clientDirectory = clientDirectory;
  }

  static TlsTestCertificates prepare(Path root) {
    try {
      Path normalized = root.toAbsolutePath().normalize();
      Path generation = Files.createDirectories(normalized.resolve("generation"));
      Path fe = Files.createDirectories(normalized.resolve("fe"));
      Path client = Files.createDirectories(normalized.resolve("client"));
      for (Path directory : List.of(normalized, generation, fe, client)) {
        Files.setPosixFilePermissions(directory, PosixFilePermissions.fromString("rwxr-xr-x"));
      }
      return new TlsTestCertificates(normalized, generation, fe, client);
    } catch (IOException e) {
      throw new IllegalStateException("Unable to prepare TLS fixture directories", e);
    }
  }

  void generate() {
    if (Files.exists(clientTrustStore())) {
      throw new IllegalStateException("TLS fixtures must be generated in a fresh test directory");
    }
    Path caKeys = generationDirectory.resolve("ca-keys.p12");
    Path caCertificate = generationDirectory.resolve("ca-certificate.pem");
    runKeytool(
        "-genkeypair",
        "-alias",
        "governed-doris-it-ca",
        "-dname",
        "CN=Governed Doris Integration Test CA",
        "-keyalg",
        "RSA",
        "-keysize",
        "2048",
        "-validity",
        "3650",
        "-ext",
        "BC=ca:true",
        "-ext",
        "KU=keyCertSign,cRLSign",
        "-keystore",
        caKeys.toString(),
        "-storetype",
        STORE_TYPE,
        "-storepass",
        FE_STORE_PASSWORD,
        "-keypass",
        FE_STORE_PASSWORD,
        "-noprompt");
    runKeytool(
        "-exportcert",
        "-rfc",
        "-alias",
        "governed-doris-it-ca",
        "-keystore",
        caKeys.toString(),
        "-storetype",
        STORE_TYPE,
        "-storepass",
        FE_STORE_PASSWORD,
        "-file",
        caCertificate.toString());

    importCa(caCertificate, feCaStore(), FE_STORE_PASSWORD, STORE_TYPE);
    createPasswordlessClientTrustStore(caCertificate);

    List<String> sanEntries = new ArrayList<>();
    for (int address = 1; address < 15; address++) {
      sanEntries.add("IP:10.20.30." + address);
    }
    sanEntries.add("IP:127.0.0.1");
    sanEntries.add("DNS:doris-fe");
    sanEntries.add("DNS:localhost");
    String san = "SAN=" + String.join(",", sanEntries);
    createServerStore("valid", null, "3650", san, feDirectory.resolve("server_certificate.p12"));
    createServerStore(
        "expired", "-3d", "1", san, feDirectory.resolve("expired-server-certificate.p12"));
    createUnknownCaServerStore(san);
    setFixturePermissions();
  }

  Path feDirectory() {
    return feDirectory;
  }

  Path clientDirectory() {
    return clientDirectory;
  }

  Path clientTrustStore() {
    return clientDirectory.resolve("client-truststore.jks");
  }

  private Path feCaStore() {
    return feDirectory.resolve("ca-certificate.p12");
  }

  private void createServerStore(
      String name, String startDate, String validity, String san, Path destination) {
    Path serverStore = generationDirectory.resolve(name + "-server-keys.p12");
    Path request = generationDirectory.resolve(name + "-server.csr");
    Path certificate = generationDirectory.resolve(name + "-server.pem");
    List<String> generate = new ArrayList<>();
    generate.addAll(
        List.of(
            "-genkeypair",
            "-alias",
            "server",
            "-dname",
            "CN=Governed Doris Integration Test FE",
            "-keyalg",
            "RSA",
            "-keysize",
            "2048"));
    if (startDate != null) {
      generate.add("-startdate");
      generate.add(startDate);
    }
    generate.addAll(
        List.of(
            "-validity",
            validity,
            "-ext",
            san,
            "-ext",
            "KU=digitalSignature,keyEncipherment",
            "-ext",
            "EKU=serverAuth",
            "-keystore",
            serverStore.toString(),
            "-storetype",
            STORE_TYPE,
            "-storepass",
            FE_STORE_PASSWORD,
            "-keypass",
            FE_STORE_PASSWORD,
            "-noprompt"));
    runKeytool(generate.toArray(String[]::new));
    runKeytool(
        "-certreq",
        "-alias",
        "server",
        "-keystore",
        serverStore.toString(),
        "-storetype",
        STORE_TYPE,
        "-storepass",
        FE_STORE_PASSWORD,
        "-file",
        request.toString(),
        "-ext",
        san);

    List<String> sign = new ArrayList<>();
    sign.addAll(
        List.of(
            "-gencert",
            "-rfc",
            "-alias",
            "governed-doris-it-ca",
            "-keystore",
            generationDirectory.resolve("ca-keys.p12").toString(),
            "-storetype",
            STORE_TYPE,
            "-storepass",
            FE_STORE_PASSWORD,
            "-infile",
            request.toString(),
            "-outfile",
            certificate.toString()));
    if (startDate != null) {
      sign.add("-startdate");
      sign.add(startDate);
    }
    sign.addAll(
        List.of(
            "-validity",
            validity,
            "-ext",
            san,
            "-ext",
            "KU=digitalSignature,keyEncipherment",
            "-ext",
            "EKU=serverAuth"));
    runKeytool(sign.toArray(String[]::new));
    runKeytool(
        "-importcert",
        "-noprompt",
        "-alias",
        "governed-doris-it-ca",
        "-file",
        generationDirectory.resolve("ca-certificate.pem").toString(),
        "-keystore",
        serverStore.toString(),
        "-storetype",
        STORE_TYPE,
        "-storepass",
        FE_STORE_PASSWORD);
    runKeytool(
        "-importcert",
        "-noprompt",
        "-alias",
        "server",
        "-file",
        certificate.toString(),
        "-keystore",
        serverStore.toString(),
        "-storetype",
        STORE_TYPE,
        "-storepass",
        FE_STORE_PASSWORD);
    try {
      Files.copy(serverStore, destination);
    } catch (IOException e) {
      throw new IllegalStateException("Unable to install the Doris FE TLS fixture", e);
    }
  }

  private void importCa(Path certificate, Path store, String password, String storeType) {
    runKeytool(
        "-importcert",
        "-noprompt",
        "-alias",
        "governed-doris-it-ca",
        "-file",
        certificate.toString(),
        "-keystore",
        store.toString(),
        "-storetype",
        storeType,
        "-storepass",
        password);
  }

  private void createPasswordlessClientTrustStore(Path caCertificate) {
    try {
      KeyStore trustStore = KeyStore.getInstance("JKS");
      trustStore.load(null, null);
      try (InputStream input = Files.newInputStream(caCertificate)) {
        trustStore.setCertificateEntry(
            "governed-doris-it-ca",
            CertificateFactory.getInstance("X.509").generateCertificate(input));
      }
      try (OutputStream output = Files.newOutputStream(clientTrustStore())) {
        trustStore.store(output, new char[0]);
      }
    } catch (IOException | GeneralSecurityException e) {
      throw new IllegalStateException("Unable to create the passwordless JVM truststore", e);
    }
  }

  private void createUnknownCaServerStore(String san) {
    Path store = feDirectory.resolve("unknown-ca-server-certificate.p12");
    runKeytool(
        "-genkeypair",
        "-alias",
        "server",
        "-dname",
        "CN=Unknown CA Doris Integration Test FE",
        "-keyalg",
        "RSA",
        "-keysize",
        "2048",
        "-validity",
        "3650",
        "-ext",
        san,
        "-ext",
        "KU=digitalSignature,keyEncipherment",
        "-ext",
        "EKU=serverAuth",
        "-keystore",
        store.toString(),
        "-storetype",
        STORE_TYPE,
        "-storepass",
        FE_STORE_PASSWORD,
        "-keypass",
        FE_STORE_PASSWORD,
        "-noprompt");
  }

  private void setFixturePermissions() {
    try {
      for (Path directory : List.of(root, generationDirectory, feDirectory, clientDirectory)) {
        Files.setPosixFilePermissions(directory, PosixFilePermissions.fromString("rwxr-xr-x"));
      }
      try (java.util.stream.Stream<Path> paths = Files.walk(root)) {
        paths
            .filter(Files::isRegularFile)
            .forEach(
                path -> {
                  try {
                    Files.setPosixFilePermissions(
                        path,
                        PosixFilePermissions.fromString(
                            path.startsWith(generationDirectory) ? "rw-------" : "rw-r--r--"));
                  } catch (IOException e) {
                    throw new IllegalStateException("Unable to set TLS fixture permissions", e);
                  }
                });
      }
    } catch (IOException e) {
      throw new IllegalStateException("Unable to set TLS fixture permissions", e);
    }
  }

  private static void runKeytool(String... arguments) {
    Path keytool = Path.of(System.getProperty("java.home"), "bin", "keytool");
    List<String> command = new ArrayList<>(arguments.length + 1);
    command.add(keytool.toString());
    command.addAll(List.of(arguments));
    ProcessBuilder processBuilder = new ProcessBuilder(command);
    processBuilder.redirectErrorStream(true);
    try {
      Process process = processBuilder.start();
      String output =
          new String(
              process.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
      int exitCode = process.waitFor();
      if (exitCode != 0) {
        throw new IllegalStateException(
            "JDK keytool failed while generating a TLS integration-test fixture: "
                + sanitize(output));
      }
    } catch (IOException e) {
      throw new IllegalStateException("Unable to execute JDK keytool for TLS fixtures", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while generating TLS fixtures", e);
    }
  }

  private static String sanitize(String output) {
    if (output == null || output.isBlank()) {
      return "no diagnostic output";
    }
    return output.replace(FE_STORE_PASSWORD, "<redacted>").replaceAll("[\\r\\n]+", " ").trim();
  }
}
