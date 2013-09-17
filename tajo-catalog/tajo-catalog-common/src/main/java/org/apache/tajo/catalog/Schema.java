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

package org.apache.tajo.catalog;

import com.google.gson.annotations.Expose;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.exception.AlreadyExistsFieldException;
import org.apache.tajo.catalog.json.CatalogGsonHelper;
import org.apache.tajo.catalog.proto.CatalogProtos.ColumnProto;
import org.apache.tajo.catalog.proto.CatalogProtos.SchemaProto;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.util.TUtil;

import java.util.*;

public class Schema implements ProtoObject<SchemaProto>, Cloneable, GsonObject {
  private static final Log LOG = LogFactory.getLog(Schema.class);
	private	SchemaProto.Builder builder = SchemaProto.newBuilder();

	@Expose protected List<Column> fields = null;
	@Expose protected Map<String, Integer> fieldsByQialifiedName = null;
  @Expose protected Map<String, List<Integer>> fieldsByName = null;

	public Schema() {
    this.fields = new ArrayList<Column>();
    this.fieldsByQialifiedName = new TreeMap<String, Integer>();
    this.fieldsByName = new HashMap<String, List<Integer>>();
	}
	
	public Schema(SchemaProto proto) {
    this.fields = new ArrayList<Column>();
    this.fieldsByQialifiedName = new HashMap<String, Integer>();
    this.fieldsByName = new HashMap<String, List<Integer>>();
    for(ColumnProto colProto : proto.getFieldsList()) {
      fields.add(new Column(colProto));
      if (colProto.hasQualifier()) {
        fieldsByQialifiedName.put(colProto.getQualifier() + "." + colProto.getColumnName(), fields.size() - 1);
      } else {
        fieldsByQialifiedName.put(colProto.getColumnName(), fields.size() - 1);
      }
      if (fieldsByName.containsKey(colProto.getColumnName())) {
        fieldsByName.get(colProto.getColumnName()).add(fields.size() - 1);
      } else {
        fieldsByName.put(colProto.getColumnName(), TUtil.newList(fields.size() - 1));
      }
    }
  }

	public Schema(Schema schema) {
	  this();
		this.fields.addAll(schema.fields);
		this.fieldsByQialifiedName.putAll(schema.fieldsByQialifiedName);
    this.fieldsByName.putAll(schema.fieldsByName);
	}
	
	public Schema(Column [] columns) {
    this();
    for(Column c : columns) {
      addColumn(c);
    }
  }

  public void setQualifier(String qualifier) {
    fieldsByQialifiedName.clear();

    for (int i = 0; i < getColumnNum(); i++) {
      fields.get(i).setQualifier(qualifier);
      fieldsByQialifiedName.put(fields.get(i).getQualifiedName(), i);
    }
  }
	
	public int getColumnNum() {
		return this.fields.size();
	}

	public Column getColumnByFQN(String colName) {
		Integer cid = fieldsByQialifiedName.get(colName.toLowerCase());
		return cid != null ? fields.get(cid) : null;
	}
	
	public Column getColumnByName(String colName) {
    String normalized = colName.toLowerCase();
	  List<Integer> list = fieldsByName.get(normalized);

    if (list == null || list.size() == 0) {
      return null;
    }

    if (list.size() == 1) {
      return fields.get(list.get(0));
    } else {
      StringBuilder sb = new StringBuilder();
      boolean first = true;
      for (Integer id : list) {
        if (first) {
          first = false;
        } else {
          sb.append(", ");
        }
        sb.append(fields.get(id));
      }
      throw new RuntimeException("Ambiguous Column Name: " + sb.toString());
    }
	}
	
	public Column getColumn(int id) {
	  return fields.get(id);
	}
	
	public int getColumnId(String colName) {
	  return fieldsByQialifiedName.get(colName.toLowerCase());
	}

  public int getColumnIdByName(String colName) {
    for (Column col : fields) {
      if (col.getColumnName().equals(colName.toLowerCase())) {
        return fieldsByQialifiedName.get(col.getQualifiedName());
      }
    }
    return -1;
  }
	
	public Collection<Column> getColumns() {
		return fields;
	}
	
	public void alter(int idx, Column column) {
	  this.fields.set(idx, column);
	}
	
	public boolean contains(String colName) {
		return fieldsByQialifiedName.containsKey(colName.toLowerCase());
	}

  public synchronized Schema addColumn(String name, Type type) {
    if (type == Type.CHAR) {
      return addColumn(name, CatalogUtil.newDataTypeWithLen(type, 1));
    }
    return addColumn(name, CatalogUtil.newDataTypeWithoutLen(type));
  }

  public synchronized Schema addColumn(String name, Type type, int length) {
    return addColumn(name, CatalogUtil.newDataTypeWithLen(type, length));
  }

  public synchronized Schema addColumn(String name, DataType dataType) {
		String nomalized = name.toLowerCase();
		if(fieldsByQialifiedName.containsKey(nomalized)) {
		  LOG.error("Already exists column " + nomalized);
			throw new AlreadyExistsFieldException(nomalized);
		}
			
		Column newCol = new Column(nomalized, dataType);
		fields.add(newCol);
		fieldsByQialifiedName.put(newCol.getQualifiedName(), fields.size() - 1);
    fieldsByName.put(newCol.getColumnName(), TUtil.newList(fields.size() - 1));
		
		return this;
	}
	
	public synchronized void addColumn(Column column) {
		addColumn(column.getQualifiedName(), column.getDataType());
	}
	
	public synchronized void addColumns(Schema schema) {
    for(Column column : schema.getColumns()) {
      addColumn(column);
    }
  }

	@Override
	public boolean equals(Object o) {
		if (o instanceof Schema) {
		  Schema other = (Schema) o;
		  return getProto().equals(other.getProto());
		}
		return false;
	}
	
  @Override
  public Object clone() {
    Schema schema = new Schema(toArray());
    return schema;
  }

	@Override
	public SchemaProto getProto() {
    builder.clearFields();
    if (this.fields  != null) {
      for(Column col : fields) {
        builder.addFields(col.getProto());
      }
    }
    return builder.build();
	}

	public String toString() {
	  StringBuilder sb = new StringBuilder();
	  sb.append("{");
	  int i = 0;
	  for(Column col : fields) {
	    sb.append(col);
	    if (i < fields.size() - 1) {
	      sb.append(",");
	    }
	    i++;
	  }
	  sb.append("}");
	  
	  return sb.toString();
	}

  @Override
	public String toJson() {
	  return CatalogGsonHelper.toJson(this, Schema.class);
		
	}

  public Column [] toArray() {
    return this.fields.toArray(new Column[this.fields.size()]);
  }
}