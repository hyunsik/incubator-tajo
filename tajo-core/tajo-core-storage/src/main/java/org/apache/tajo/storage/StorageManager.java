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

package org.apache.tajo.storage;

import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.conf.TajoConf;

import java.io.IOException;

/**
 * StorageManager
 */
public class StorageManager extends AbstractStorageManager {

  protected StorageManager(TajoConf conf) throws IOException {
    super(conf);
  }

  @Override
  public Scanner getScanner(TableMeta meta, Fragment fragment,
                                   Schema target) throws IOException {
    Scanner scanner;

    Class<? extends Scanner> scannerClass;

    String handlerName = meta.getStoreType().name().toLowerCase();
    scannerClass = SCANNER_HANDLER_CACHE.get(handlerName);
    if (scannerClass == null) {
      scannerClass = conf.getClass(
          String.format("tajo.storage.scanner-handler.%s.class",
              meta.getStoreType().name().toLowerCase()), null,
          Scanner.class);
      SCANNER_HANDLER_CACHE.put(handlerName, scannerClass);
    }

    if (scannerClass == null) {
      throw new IOException("Unknown Storage Type: " + meta.getStoreType());
    }

    scanner = newScannerInstance(scannerClass, conf, meta, fragment);
    if (scanner.isProjectable()) {
      scanner.setTarget(target.toArray());
    }

    return scanner;
  }
}
