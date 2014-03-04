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

public class AlreadyExistsTableException extends CatalogException {
	private static final long serialVersionUID = -641623770742392865L;

	public AlreadyExistsTableException() {		
	}

  public AlreadyExistsTableException(String databaseName, @Nullable String namespace, String relName) {
    super(String.format("relation \" %s \" already exists in %s.%s", relName, databaseName, namespace));
  }

	public AlreadyExistsTableException(String tableName) {
		super("relation \"" + tableName + "\" exists table");
	}
}
