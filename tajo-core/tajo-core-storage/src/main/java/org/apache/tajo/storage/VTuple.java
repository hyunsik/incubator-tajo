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

import com.google.gson.annotations.Expose;
import org.apache.tajo.datum.*;
import org.apache.tajo.datum.exception.InvalidCastException;

import java.net.InetAddress;
import java.util.Arrays;

public class VTuple implements Tuple {
	@Expose public Datum [] values;
	@Expose private long offset;
	
	public VTuple(int size) {
		values = new Datum [size];
	}

  public VTuple(Tuple tuple) {
    this.values = new Datum[tuple.size()];
    System.arraycopy(((VTuple)tuple).values, 0, values, 0, tuple.size());
    this.offset = ((VTuple)tuple).offset;
  }

  public VTuple(Datum [] datum) {
    this(datum.length);
    put(datum);
  }

	@Override
	public int size() {	
		return values.length;
	}
	
	public boolean contains(int fieldId) {
		return values[fieldId] != null;
	}

  @Override
  public boolean isNull(int fieldid) {
    return values[fieldid] instanceof NullDatum;
  }

  @Override
  public void clear() {   
    for (int i=0; i < values.length; i++) {
      values[i] = null;
    }
  }
	
	//////////////////////////////////////////////////////
	// Setter
	//////////////////////////////////////////////////////	
	public void put(int fieldId, Datum value) {
		values[fieldId] = value;
	}

  @Override
  public void put(int fieldId, Datum[] values) {
    for (int i = fieldId, j = 0; j < values.length; i++, j++) {
      values[i] = values[j];
    }
  }

  @Override
  public void put(int fieldId, Tuple tuple) {
    for (int i = fieldId, j = 0; j < tuple.size(); i++, j++) {
      values[i] = tuple.get(j);
    }
  }

  public void put(Datum [] values) {
    System.arraycopy(values, 0, this.values, 0, size());
	}
	
	//////////////////////////////////////////////////////
	// Getter
	//////////////////////////////////////////////////////
	public Datum get(int fieldId) {
		return this.values[fieldId];
	}
	
	public void setOffset(long offset) {
	  this.offset = offset;
	}
	
	public long getOffset() {
	  return this.offset;
	}
	
	@Override
	public BooleanDatum getBoolean(int fieldId) {
		return (BooleanDatum) values[fieldId];
	}

	public BitDatum getByte(int fieldId) {
		return (BitDatum) values[fieldId];
	}

  public CharDatum getChar(int fieldId) {
    return (CharDatum) values[fieldId];
  }

	public BlobDatum getBytes(int fieldId) {
		return (BlobDatum) values[fieldId];
	}

	public Int2Datum getShort(int fieldId) {
		return (Int2Datum) values[fieldId];
	}

	public Int4Datum getInt(int fieldId) {
		return (Int4Datum) values[fieldId];
	}

	public Int8Datum getLong(int fieldId) {
		return (Int8Datum) values[fieldId];
	}

	public Float4Datum getFloat(int fieldId) {
		return (Float4Datum) values[fieldId];
	}

	public Float8Datum getDouble(int fieldId) {
		return (Float8Datum) values[fieldId];
	}

	public Inet4Datum getIPv4(int fieldId) {
		return (Inet4Datum) values[fieldId];
	}

	public byte[] getIPv4Bytes(int fieldId) {
		return values[fieldId].asByteArray();
	}

	public InetAddress getIPv6(int fieldId) {
		throw new InvalidCastException("IPv6 is unsupported yet");
	}

	public byte[] getIPv6Bytes(int fieldId) {
	  throw new InvalidCastException("IPv6 is unsupported yet");
	}

	public TextDatum getString(int fieldId) {
		return (TextDatum) values[fieldId];
	}

  @Override
  public TextDatum getText(int fieldId) {
    return (TextDatum) values[fieldId];
  }

  @Override
  public Tuple clone() throws CloneNotSupportedException {
    return new VTuple(this);
  }

  public String toString() {
		boolean first = true;
		StringBuilder str = new StringBuilder();
		str.append("(");
		for(int i=0; i < values.length; i++) {			
			if(values[i] != null) {
				if(first) {
					first = false;
				} else {
					str.append(", ");
				}
				str.append(i)
				.append("=>")
				.append(values[i]);
			}
		}
		str.append(")");
		return str.toString();
	}
	
	@Override
	public int hashCode() {
	  int hashCode = 37;
	  for (int i=0; i < values.length; i++) {
	    if(values[i] != null) {
        hashCode ^= (values[i].hashCode() * 41);
	    } else {
	      hashCode = hashCode ^ (i + 17);
	    }
	  }
	  
	  return hashCode;
	}

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof VTuple) {
      VTuple other = (VTuple) obj;
      return Arrays.equals(values, other.values);
    } else if (obj instanceof LazyTuple) {
      LazyTuple other = (LazyTuple) obj;
      return Arrays.equals(values, other.toArray());
    }

    return false;
  }
}
