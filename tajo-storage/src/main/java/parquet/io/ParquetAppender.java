package parquet.io;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.storage.FileAppender;
import org.apache.tajo.storage.Tuple;
import parquet.hadoop.ParquetFileWriter;
import parquet.io.api.Binary;
import parquet.io.api.RecordConsumer;
import parquet.schema.GroupType;
import parquet.schema.Type;

import java.io.IOException;

public class ParquetAppender extends FileAppender {
  private final static Log logger = LogFactory.getLog(ParquetAppender.class);
  private final static boolean DEBUG = true;
  private RecordConsumer recordConsumer = null;

  public ParquetAppender(Configuration conf, Schema schema, TableMeta meta, Path path) {
    super(conf, schema, meta, path);

    ParquetFileWriter writer = null;
  }

  @Override
  public void init() {

  }

  @Override
  public long getOffset() throws IOException {
    return 0;
  }

  @Override
  public void addTuple(Tuple t) throws IOException {
    recordConsumer.startMessage();


    final GroupType type = null;
    for (int fieldId = 0; fieldId < schema.size(); fieldId++) {
      final Type fieldType = type.getType(fieldId);
      Column column = schema.getColumn(fieldId);

      recordConsumer.startField(column.getQualifiedName(), fieldId);

      switch(column.getDataType().getType()) {
      case BOOLEAN:
        recordConsumer.addBoolean(t.getBool(fieldId));
        break;
      case INT1:
      case INT2:
      case INT4:
        recordConsumer.addInteger(t.getInt4(fieldId));
        break;
      case INT8:
        recordConsumer.addLong(t.getInt8(fieldId));
        break;
      case FLOAT4:
        recordConsumer.addFloat(t.getFloat4(fieldId));
        break;
      case FLOAT8:
        recordConsumer.addDouble(t.getFloat8(fieldId));
        break;
      case TEXT:
      case BLOB:
        recordConsumer.addBinary(Binary.fromByteArray(t.getBytes(fieldId)));
        break;
      }

      recordConsumer.endField("", fieldId);
    }

    recordConsumer.endMessage();
  }

  @Override
  public void flush() throws IOException {

  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public TableStats getStats() {
    return null;
  }
}
