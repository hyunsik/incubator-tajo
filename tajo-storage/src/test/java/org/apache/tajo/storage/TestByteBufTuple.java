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


import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.DatumFactory;
import org.junit.Before;
import org.junit.Test;

import static org.apache.tajo.common.TajoDataTypes.Type;
import static org.junit.Assert.*;

public class TestByteBufTuple {
	@Before
	public void setUp() throws Exception {

	}
	
	@Test
	public void testContain() {
    Schema schema = new Schema();
    for (int i = 0; i < 260; i++) {
      schema.addColumn("col_" + i, Type.INT4);
    }
    TupleFactory factory = new TupleFactory(true, schema);

		Tuple t1 = factory.newByteBufTuple();
		t1.put(0, DatumFactory.createInt4(1));
		t1.put(1, DatumFactory.createInt4(1));
		t1.put(27, DatumFactory.createInt4(1));
		t1.put(96, DatumFactory.createInt4(1));
		t1.put(257, DatumFactory.createInt4(1));
		
		assertTrue(t1.contains(0));
		assertTrue(t1.contains(1));
		assertFalse(t1.contains(2));
		assertFalse(t1.contains(3));
		assertFalse(t1.contains(4));
		assertTrue(t1.contains(27));
		assertFalse(t1.contains(28));
		assertFalse(t1.contains(95));
		assertTrue(t1.contains(96));
		assertFalse(t1.contains(97));
		assertTrue(t1.contains(257));
	}
	
	@Test
	public void testPut() {
    Schema schema = new Schema();
    schema.addColumn(new Column("col1", Type.TEXT));
    schema.addColumn(new Column("col2", Type.INT4));
    schema.addColumn(new Column("col3", Type.FLOAT4));
    TupleFactory factory = new TupleFactory(true, schema);

		Tuple t1 = factory.newByteBufTuple();
		t1.put(0, DatumFactory.createText("str"));
		t1.put(1, DatumFactory.createInt4(2));
		t1.put(2, DatumFactory.createFloat4(0.76f));
		
		assertTrue(t1.contains(0));
		assertTrue(t1.contains(1));
		
		assertEquals(t1.getText(0),"str");
		assertEquals(t1.get(1).asInt4(),2);
		assertTrue(t1.get(2).asFloat4() == 0.76f);
	}

  @Test
	public void testEquals() {
    Schema schema = new Schema();
    schema.addColumn(new Column("col1", Type.INT4));
    schema.addColumn(new Column("col2", Type.INT4));
    schema.addColumn(new Column("col3", Type.INT4));
    schema.addColumn(new Column("col4", Type.INT4));
    schema.addColumn(new Column("col5", Type.INT4));
    TupleFactory factory = new TupleFactory(true, schema);

    Tuple t1 = factory.newByteBufTuple();
	  Tuple t2 = factory.newByteBufTuple();
	  
	  t1.put(0, DatumFactory.createInt4(1));
	  t1.put(1, DatumFactory.createInt4(2));
	  t1.put(3, DatumFactory.createInt4(2));
	  
	  t2.put(0, DatumFactory.createInt4(1));
    t2.put(1, DatumFactory.createInt4(2));
    t2.put(3, DatumFactory.createInt4(2));
    
    assertEquals(t1,t2);
    
    Tuple t3 =  factory.newByteBufTuple();
    t2.put(0, DatumFactory.createInt4(1));
    t2.put(1, DatumFactory.createInt4(2));
    t2.put(4, DatumFactory.createInt4(2));
    
    assertNotSame(t1,t3);
	}
	
	@Test
	public void testHashCode() {
    Schema schema = new Schema();
    schema.addColumn(new Column("col1", Type.INT4));
    schema.addColumn(new Column("col2", Type.INT4));
    schema.addColumn(new Column("col3", Type.INT4));
    schema.addColumn(new Column("col4", Type.INT4));
    schema.addColumn(new Column("col5", Type.TEXT));
    TupleFactory factory = new TupleFactory(true, schema);
	  ByteBufTuple t1 = factory.newByteBufTuple();
    ByteBufTuple t2 = factory.newByteBufTuple();
    
    t1.put(0, DatumFactory.createInt4(1));
    t1.put(1, DatumFactory.createInt4(2));
    t1.put(3, DatumFactory.createInt4(2));
    t1.put(4, DatumFactory.createText("tajo"));
    
    t2.put(0, DatumFactory.createInt4(1));
    t2.put(1, DatumFactory.createInt4(2));
    t2.put(3, DatumFactory.createInt4(2));
    t2.put(4, DatumFactory.createText("tajo"));
    
    assertEquals(t1.hashCode(),t2.hashCode());
    
    Tuple t3 = factory.newByteBufTuple();
    t3.put(0, DatumFactory.createInt4(1));
    t3.put(1, DatumFactory.createInt4(2));
    t3.put(4, DatumFactory.createText("Tajo"));
    
    assertNotSame(t1.hashCode(),t3.hashCode());
	}

  @Test
  public void testPutTuple() {
    Schema schema = new Schema();
    schema.addColumn(new Column("col1", Type.INT4));
    schema.addColumn(new Column("col2", Type.INT4));
    schema.addColumn(new Column("col3", Type.INT4));
    schema.addColumn(new Column("col4", Type.INT4));
    schema.addColumn(new Column("col5", Type.INT4));
    TupleFactory factory = new TupleFactory(true, schema);

    Tuple t1 = factory.newByteBufTuple();

    t1.put(0, DatumFactory.createInt4(1));
    t1.put(1, DatumFactory.createInt4(2));
    t1.put(2, DatumFactory.createInt4(3));

    Tuple t2 = new VTuple(2);
    t2.put(0, DatumFactory.createInt4(4));
    t2.put(1, DatumFactory.createInt4(5));

    t1.put(3, t2);

    for (int i = 0; i < 5; i++) {
      assertEquals(i+1, t1.get(i).asInt4());
    }
  }

  @Test
  public void testClone() throws CloneNotSupportedException {
    Schema schema = new Schema();
    schema.addColumn(new Column("col1", Type.INT4));
    schema.addColumn(new Column("col2", Type.INT4));
    schema.addColumn(new Column("col3", Type.INT4));
    schema.addColumn(new Column("col4", Type.INT4));
    schema.addColumn(new Column("col5", Type.TEXT));
    TupleFactory factory = new TupleFactory(true, schema);

    Tuple t1 = factory.newByteBufTuple();
    t1.put(0, DatumFactory.createInt4(1));
    t1.put(1, DatumFactory.createInt4(2));
    t1.put(3, DatumFactory.createInt4(2));
    t1.put(4, DatumFactory.createText("str"));

    Tuple t2 = t1.clone();
    assertEquals(t1, t2);

    assertNotSame(t1.get(4), t2.get(4));
    assertEquals(t1.get(4), t2.get(4));
  }

  @Test
  public void testGetMemorySize() throws CloneNotSupportedException {
    Schema schema = new Schema();
    schema.addColumn(new Column("col1", Type.INT4));
    schema.addColumn(new Column("col2", Type.INT4));
    schema.addColumn(new Column("col3", Type.INT4));
    schema.addColumn(new Column("col4", Type.INT4));
    schema.addColumn(new Column("col5", Type.TEXT));
    TupleFactory factory = new TupleFactory(true, schema);

    ByteBufTuple t1 = factory.newByteBufTuple();
    t1.put(0, DatumFactory.createInt4(1));
    t1.put(1, DatumFactory.createInt4(2));
    t1.put(3, DatumFactory.createInt4(2));
    t1.put(4, DatumFactory.createText("str"));

    Tuple t2 = t1.clone();

    System.out.println(t1.getMemorySize());
  }
}
