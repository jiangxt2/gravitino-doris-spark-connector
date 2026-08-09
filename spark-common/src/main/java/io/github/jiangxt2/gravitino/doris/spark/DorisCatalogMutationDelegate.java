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

import java.util.Map;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NonEmptyNamespaceException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;

/** Stable extension seam for future Gravitino-authorized Doris catalog mutations. */
public interface DorisCatalogMutationDelegate {

  /** Creates a table after the delegate has performed the required Gravitino authorization. */
  Table createTable(
      Identifier identifier,
      StructType schema,
      Transform[] partitions,
      Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException;

  /** Alters a table after the delegate has performed the required Gravitino authorization. */
  Table alterTable(Identifier identifier, TableChange... changes) throws NoSuchTableException;

  /** Drops a table after the delegate has performed the required Gravitino authorization. */
  boolean dropTable(Identifier identifier);

  /** Purges a table after the delegate has performed the required Gravitino authorization. */
  boolean purgeTable(Identifier identifier);

  /** Renames a table after the delegate has performed the required Gravitino authorization. */
  void renameTable(Identifier oldIdentifier, Identifier newIdentifier)
      throws NoSuchTableException, TableAlreadyExistsException;

  /** Creates a namespace after the delegate has performed the required Gravitino authorization. */
  void createNamespace(String[] namespace, Map<String, String> metadata)
      throws NamespaceAlreadyExistsException;

  /** Alters a namespace after the delegate has performed the required Gravitino authorization. */
  void alterNamespace(String[] namespace, NamespaceChange... changes)
      throws NoSuchNamespaceException;

  /** Drops a namespace after the delegate has performed the required Gravitino authorization. */
  boolean dropNamespace(String[] namespace, boolean cascade)
      throws NoSuchNamespaceException, NonEmptyNamespaceException;

  /** Returns the initial read-only delegate. */
  static DorisCatalogMutationDelegate readOnly(DorisCapabilityPolicy policy) {
    return new DorisCatalogMutationDelegate() {
      @Override
      public Table createTable(
          Identifier identifier,
          StructType schema,
          Transform[] partitions,
          Map<String, String> properties) {
        throw policy.reject("create table");
      }

      @Override
      public Table alterTable(Identifier identifier, TableChange... changes) {
        throw policy.reject("alter table");
      }

      @Override
      public boolean dropTable(Identifier identifier) {
        throw policy.reject("drop table");
      }

      @Override
      public boolean purgeTable(Identifier identifier) {
        throw policy.reject("purge table");
      }

      @Override
      public void renameTable(Identifier oldIdentifier, Identifier newIdentifier) {
        throw policy.reject("rename table");
      }

      @Override
      public void createNamespace(String[] namespace, Map<String, String> metadata) {
        throw policy.reject("create namespace");
      }

      @Override
      public void alterNamespace(String[] namespace, NamespaceChange... changes) {
        throw policy.reject("alter namespace");
      }

      @Override
      public boolean dropNamespace(String[] namespace, boolean cascade) {
        throw policy.reject("drop namespace");
      }
    };
  }
}
