package org.apache.tajo.jdbc;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestTajoResultSet {
  @Test
  public final void testFileNameComparator() {

    Path[] paths = new Path [] {
        new Path("hdfs://xtajox.com:9010/tmp/tajo-hadoop/staging/q_1391511584109_0001/RESULT/part-02-000104"),
        new Path("hdfs://xtajox.com:9010/tmp/tajo-hadoop/staging/q_1391511584109_0001/RESULT/part-02-000000"),
        new Path("hdfs://xtajox.com:9010/tmp/tajo-hadoop/staging/q_1391511584109_0001/RESULT/part-02-000105"),
        new Path("hdfs://xtajox.com:9010/tmp/tajo-hadoop/staging/q_1391511584109_0001/RESULT/part-02-000001")
    };

    FileStatus [] fileStatuses = new FileStatus[paths.length];

    for (int i = 0; i < paths.length; i++) {
      fileStatuses[i] = mock(FileStatus.class);
      when(fileStatuses[i].getPath()).thenReturn(paths[i]);
    }

    TajoResultSet.FileNameComparator comparator = new TajoResultSet.FileNameComparator();
    Arrays.sort(fileStatuses, comparator);

    FileStatus prev = null;
    for (int i = 0; i < fileStatuses.length; i++) {
      if (prev == null) {
        prev = fileStatuses[i];
      } else {
        assertTrue(comparator.compare(prev, fileStatuses[i]) <= 0);
      }
    }
  }
}
