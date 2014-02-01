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
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.util.Bytes;
import org.apache.tajo.util.ClassSize;

/**
 * Immutable Tuple
 */
public class ByteBufTuple implements Tuple {
  private final short num;
  private final Schema schema;
  private final int [] offsets;
  private final ByteBuf byteBuf;

  private short currentIdx = 0;

  private static final int staticBytesSize;
  static {
    staticBytesSize =
            ClassSize.REFERENCE +   // this
            Bytes.SIZEOF_SHORT +    // num
            ClassSize.REFERENCE +   // schema
            ClassSize.ARRAY +       // offsets
            ClassSize.REFERENCE +   // bytebuf
            Bytes.SIZEOF_SHORT;     // currentIdx
  }

  public ByteBufTuple(Schema schema, ByteBuf bytebuf) {
    this.schema = schema;
    this.num = (short) schema.getColumnNum();
    this.offsets = new int[this.num];
    this.byteBuf = bytebuf;
  }

  public void release() {
    byteBuf.release();
  }

  public int getMemorySize() {
    return staticBytesSize +
        (Bytes.SIZEOF_INT * num) + // the contents of offsets
        byteBuf.writerIndex();     // the contents of byteBuf
  }

  @Override
  public int size() {
    return num;
  }

  @Override
  public boolean contains(int fieldid) {
    return offsets[fieldid] > -1;
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

    if (currentIdx < fieldId) {
      for (;currentIdx < fieldId; currentIdx++) {
        offsets[currentIdx] = -1;
      }
    }

    int offset = byteBuf.writerIndex();

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
      //writeRawVarint32(value.asInt4());
      byteBuf.writeInt(value.asInt4());
      break;

    case DATE:
    case TIMESTAMP:
    case INT8:
      //writeRawVarint64(value.asInt8());
      byteBuf.writeLong(value.asInt8());
      break;

    case FLOAT4:
      //writeRawLittleEndian32(Float.floatToRawIntBits(value.asFloat4()));
      byteBuf.writeFloat(value.asFloat4());
      break;

    case FLOAT8:
      //writeRawLittleEndian64(Double.doubleToRawLongBits(value.asFloat8()));
      byteBuf.writeDouble(value.asFloat8());
      break;

    case TEXT:
      //writeRawVarint32(value.size());
      //byteBuf.writeBytes(value.asByteArray());
      byte [] bytes = value.asByteArray();
      byteBuf.writeInt(bytes.length);
      byteBuf.writeBytes(bytes);
      break;

    case NULL_TYPE:
      offsets[currentIdx++] = -1;
      return;
    }

    if (currentIdx < num) {
      offsets[currentIdx++] = offset;
    }
  }

  @Override
  public void put(int fieldId, Datum[] values) {
    throw new UnsupportedException("put(int, Datum[]) is not supported in " + ByteBufTuple.class.getSimpleName());
  }

  @Override
  public void put(int fieldId, Tuple tuple) {
    for (int i = 0, j = currentIdx; i < tuple.size(); i++,  j++) {
      put(j, tuple.get(i));
    }
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
      return DatumFactory.createBool(getBool(fieldId));
    case BIT:
      return DatumFactory.createBit(getByte(fieldId));
    case CHAR:
      return DatumFactory.createChar(getChar(fieldId));
    case INT1:
    case INT2:
      return DatumFactory.createInt2(getShort(fieldId));
    case INT4:
      return DatumFactory.createInt4(getInt(fieldId));
    case INT8:
      return DatumFactory.createInt8(getLong(fieldId));
    case FLOAT4:
      return DatumFactory.createFloat4(getFloat(fieldId));
    case FLOAT8:
      return DatumFactory.createFloat8(getDouble(fieldId));
    case TEXT:
      return DatumFactory.createText(getText(fieldId));
    case BLOB:
      return DatumFactory.createBlob(getBytes(fieldId));
    default:
      throw new UnsupportedException(column.getDataType().getType() + " is not supported in "
          + ByteBufTuple.class.getSimpleName());
    }
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
  public String getText(int fieldId) {
    int len = byteBuf.getInt(offsets[fieldId]);
    byte [] bytes = new byte[len];
    byteBuf.getBytes(offsets[fieldId] + 4, bytes);
    return new String(bytes);
  }

  @Override
  public Tuple clone() throws CloneNotSupportedException {
    byteBuf.retain();
    return this;
  }

  @Override
  public Datum[] getValues() {
    throw new UnsupportedException("getValues() is not supported in " + ByteBufTuple.class.getSimpleName());
  }

  public boolean equals(Object object) {
    if (object instanceof ByteBufTuple) {
      ByteBufTuple another = (ByteBufTuple) object;
      return byteBuf.equals(another.byteBuf);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return byteBuf.hashCode();
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
