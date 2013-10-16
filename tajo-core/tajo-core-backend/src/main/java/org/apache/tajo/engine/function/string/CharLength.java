package org.apache.tajo.engine.function.string;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.engine.function.GeneralFunction;
import org.apache.tajo.storage.Tuple;

/**
 * Function definition
 *
 * INT4 char_length(string text) or INT4 character_length(string text)
 */
public class CharLength extends GeneralFunction {
  public CharLength() {
    super(new Column[] {
        new Column("text", TajoDataTypes.Type.TEXT)
    });
  }

  @Override
  public Datum eval(Tuple params) {
    Datum datum = params.get(0);
    if(datum instanceof NullDatum) return NullDatum.get();

    return DatumFactory.createInt4(datum.asChars().length());
  }
}
