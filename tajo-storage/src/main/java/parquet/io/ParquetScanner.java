package parquet.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.storage.FileScanner;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.fragment.FileFragment;

import java.io.IOException;

public class ParquetScanner extends FileScanner {

  public ParquetScanner(Configuration conf, Schema schema, TableMeta meta, FileFragment fragment) {
    super(conf, schema, meta, fragment);
  }

  @Override
  public Tuple next() throws IOException {
    return null;
  }

  @Override
  public void reset() throws IOException {

  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public boolean isProjectable() {
    return false;
  }

  @Override
  public boolean isSelectable() {
    return false;
  }

  @Override
  public boolean isSplittable() {
    return false;
  }
}
