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

import io.netty.buffer.ByteBuf;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.exception.UnsupportedException;

public class ByteBufTuple implements Tuple {
  private final short num;
  private final Schema schema;

  private short currentIdx = 0;
  private int [] offsets;
  private ByteBuf byteBuf;

  public ByteBufTuple(Schema schema) {
    this.schema = schema;
    this.num = (short) schema.getColumnNum();
    offsets = new int[this.num];
  }

  @Override
  public int size() {
    return byteBuf.writerIndex();
  }

  @Override
  public boolean contains(int fieldid) {
    return offsets[fieldid] > 0;
  }

  @Override
  public boolean isNull(int fieldid) {
    return offsets[fieldid] == -1;
  }

  @Override
  public void clear() {
    throw new UnsupportedException("clear() is not supported");
  }

  @Override
  public void put(int fieldId, Datum value) {
    switch (value.type()) {
    case BOOLEAN:
      byteBuf.writeBoolean(value.asBool());
      break;

    case INT1:
      byteBuf.writeByte(value.asInt2());
      break;

    case INT2:
    case INT4:
    case TIME:
      writeRawVarint32(value.asInt4());
      break;

    case DATE:
    case TIMESTAMP:
    case INT8:
      writeRawVarint64(value.asInt8());
      break;

    case FLOAT4:
      writeRawLittleEndian32(Float.floatToRawIntBits(value.asFloat4()));
      break;

    case FLOAT8:
      writeRawLittleEndian64(Double.doubleToRawLongBits(value.asFloat8()));
      break;

    case TEXT:
      writeRawVarint32(value.size());
      byteBuf.writeBytes(value.asByteArray());
      break;
    }

    if (currentIdx < num) {
      offsets[currentIdx++] = byteBuf.writerIndex();
    }
  }

  @Override
  public void put(int fieldId, Datum[] values) {

  }

  @Override
  public void put(int fieldId, Tuple tuple) {

  }

  @Override
  public void put(Datum[] values) {
    for (int i = 0; i < values.length; i++) {
      put(i, values[i]);
    }
  }

  @Override
  public Datum get(int fieldId) {
    Column column = schema.getColumn(fieldId);
    switch (column.getDataType().getType()) {
    case BOOLEAN:
    case BIT:
    case CHAR:
    case INT1:
    case INT2:
    case INT4:
    case INT8:
    case FLOAT4:
    case FLOAT8:
    case TEXT:
    case BLOB:
    }
    return null;
  }

  @Override
  public void setOffset(long offset) {

  }

  @Override
  public long getOffset() {
    return 0;
  }

  @Override
  public boolean getBool(int fieldId) {
    return byteBuf.getBoolean(offsets[fieldId]);
  }

  @Override
  public byte getByte(int fieldId) {
    return byteBuf.getByte(offsets[fieldId]);
  }

  @Override
  public char getChar(int fieldId) {
    return byteBuf.getChar(offsets[fieldId]);
  }

  @Override
  public byte [] getBytes(int fieldId) {
    int len = byteBuf.getInt(offsets[fieldId]);
    byte [] bytes = new byte[len];
    byteBuf.getBytes(offsets[fieldId] + 4, bytes);
    return bytes;
  }

  @Override
  public short getShort(int fieldId) {
    return byteBuf.getShort(offsets[fieldId]);
  }

  @Override
  public int getInt(int fieldId) {
    return byteBuf.getInt(offsets[fieldId]);
  }

  @Override
  public long getLong(int fieldId) {
    return byteBuf.getLong(offsets[fieldId]);
  }

  @Override
  public float getFloat(int fieldId) {
    return byteBuf.getFloat(offsets[fieldId]);
  }

  @Override
  public double getDouble(int fieldId) {
    return byteBuf.getDouble(offsets[fieldId]);
  }

  @Override
  public String getString(int fieldId) {
    int len = byteBuf.getInt(offsets[fieldId]);
    byte [] bytes = new byte[len];
    byteBuf.getBytes(offsets[fieldId] + 4, bytes);
    return new String(bytes);
  }

  @Override
  public Tuple clone() throws CloneNotSupportedException {
    return null;
  }

  @Override
  public Datum[] getValues() {
    return new Datum[0];
  }

  /** Write a little-endian 32-bit integer. */
  public void writeRawLittleEndian32(final int value) {
    byteBuf.writeByte((value) & 0xFF);
    byteBuf.writeByte((value >> 8) & 0xFF);
    byteBuf.writeByte((value >> 16) & 0xFF);
    byteBuf.writeByte((value >> 24) & 0xFF);
  }

  /** Write a little-endian 64-bit integer. */
  public void writeRawLittleEndian64(final long value) {
    byteBuf.writeByte((int) (value) & 0xFF);
    byteBuf.writeByte((int) (value >> 8) & 0xFF);
    byteBuf.writeByte((int) (value >> 16) & 0xFF);
    byteBuf.writeByte((int) (value >> 24) & 0xFF);
    byteBuf.writeByte((int) (value >> 32) & 0xFF);
    byteBuf.writeByte((int) (value >> 40) & 0xFF);
    byteBuf.writeByte((int) (value >> 48) & 0xFF);
    byteBuf.writeByte((int) (value >> 56) & 0xFF);
  }

  /**
   * Encode and write a varint.  {@code value} is treated as
   * unsigned, so it won't be sign-extended if negative.
   */
  public void writeRawVarint32(int value) {
    while (true) {
      if ((value & ~0x7F) == 0) {
        byteBuf.writeByte(value);
        return;
      } else {
        byteBuf.writeByte((value & 0x7F) | 0x80);
        value >>>= 7;
      }
    }
  }

  /** Encode and write a varint. */
  public void writeRawVarint64(long value) {
    while (true) {
      if ((value & ~0x7FL) == 0) {
        byteBuf.writeByte((int) value);
        return;
      } else {
        byteBuf.writeByte(((int) value & 0x7F) | 0x80);
        value >>>= 7;
      }
    }
  }
}
