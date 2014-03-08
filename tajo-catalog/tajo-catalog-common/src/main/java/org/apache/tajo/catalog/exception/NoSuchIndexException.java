/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.catalog.exception;

import org.apache.tajo.annotation.Nullable;

public class NoSuchIndexException extends CatalogException {
  private static final long serialVersionUID = 3705839985189534673L;

  /**
   * 
   */
  public NoSuchIndexException() {
  }

  public NoSuchIndexException(String databaseName, @Nullable String namespace, String columnName) {
    super(String.format("ERROR: index \" %s \" in %s.%s does not exist", columnName, databaseName, namespace));
  }

  public NoSuchIndexException(String indexName) {
    super("ERROR: index \"" + indexName + "\" does not exist");
  }
}
