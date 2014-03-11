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

package org.apache.tajo.cli;

import java.util.ArrayList;
import java.util.List;

public class SimpleScanner {

  public static List<String> scanStatements(String str) throws InvalidStatement {

    ScanState state = ScanState.TOK_START;
    int idx = 0;
    int startIdx = 0;
    int endIdx = 0;
    List<String> statements = new ArrayList<String>();
    String errorMessage = "";

    while(idx < str.length()) {

      if (Character.isWhitespace(str.charAt(idx))) {
        idx++;
        continue;
      }

      if (state == ScanState.TOK_START && str.charAt(idx) == '\\') {
        startIdx = idx;
        state = ScanState.TOK_META;

        while (state != ScanState.TOK_EOS && idx < str.length()) {

          char character = str.charAt(idx++);

          if (Character.isWhitespace(character)) {
            // skip
          } else if (character == ';') {
            state = ScanState.TOK_EOS;
          } else {
            endIdx = idx;
          }
        }

        // if the token is last without semicolon
        if (state == ScanState.TOK_META && idx == str.length()) {
          endIdx = idx;
          state = ScanState.TOK_EOS;
        }

      } else if (state == ScanState.TOK_START && Character.isLetterOrDigit(str.charAt(idx))) {
        startIdx = idx;
        state = ScanState.TOK_STATEMENT;

        while (!isTerminateState(state) && idx < str.length()) {
          char character = str.charAt(idx++);

          if (Character.isWhitespace(character)) {
            // skip
          } else if (character == '\'') {
            state = ScanState.TOK_QUOTE;
            idx++;

            while(state == ScanState.TOK_QUOTE && idx < str.length()) {
              if (str.charAt(idx) == '\'') {
                state = ScanState.TOK_STATEMENT;
              }
              idx++;
            }

            if (state != ScanState.TOK_STATEMENT) {
              errorMessage = "non-terminated quote";
              state = ScanState.TOK_INVALID;
            }
          } else if (character == ';') {
            state = ScanState.TOK_EOS;
          } else {
            endIdx = idx;
          }
        }

        // if the token is last without semicolon
        if (state == ScanState.TOK_STATEMENT && idx == str.length()) {
          endIdx = idx;
          state = ScanState.TOK_EOS;
        }
      }

      if (state == ScanState.TOK_EOS) {
        statements.add(str.subSequence(startIdx, endIdx).toString());
        state = ScanState.TOK_START;
      } else {
        throw new InvalidStatement("ERROR: " + errorMessage);
      }
    }

    return statements;
  }

  private static boolean isTerminateState(ScanState state) {
    return (state == ScanState.TOK_EOS || state == ScanState.TOK_INVALID);
  }

  enum ScanState {
    TOK_START,     // Start State
    TOK_META,      // Meta Command
    TOK_STATEMENT, // Statement
    TOK_QUOTE,     // Within Quote
    TOK_INVALID,   // Invalid Statement
    TOK_EOS        // End State (End of Statement)
  }
}
