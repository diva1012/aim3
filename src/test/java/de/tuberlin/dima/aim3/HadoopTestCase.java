/**
 * AIM3 - Scalable Data Mining -  course work
 * Copyright (C) 2014  Sebastian Schelter
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package de.tuberlin.dima.aim3;

import com.google.common.io.Closeables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.RandomUtils;
import org.junit.After;
import org.junit.Before;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class HadoopTestCase {

  public static final Double EPSILON = 0.0001;

  private File testTempDir;
  private Path testTempDirPath;
  private FileSystem fs;

  @Before
  public void setUp() throws Exception {
    RandomUtils.useTestSeed();
    testTempDir = null;
    testTempDirPath = null;
    fs = null;
  }

  @After
  public void tearDown() throws Exception {
    if (testTempDirPath != null) {
      try {
        fs.delete(testTempDirPath, true);
      } catch (IOException e) {
        throw new IllegalStateException("Test file not found");
      }
      testTempDirPath = null;
      fs = null;
    }
    if (testTempDir != null) {
      new DeletingVisitor().accept(testTempDir);
    }
  }

  protected static void writeLines(File file, String... lines) throws FileNotFoundException {
    writeLines(file, Arrays.asList(lines));
  }

  protected static void writeLines(File file, Iterable<String> lines) throws FileNotFoundException {
    PrintWriter writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(file), Charset.forName("UTF-8")));
    try {
      for (String line : lines) {
        writer.println(line);
      }
    } finally {
      writer.close();
    }
  }

  protected final Path getTestTempDirPath() throws IOException {
    if (testTempDirPath == null) {
      fs = FileSystem.get(new Configuration());
      long simpleRandomLong = (long) (Long.MAX_VALUE * Math.random());
      testTempDirPath = fs.makeQualified(
          new Path("/tmp/mahout-" + getClass().getSimpleName() + '-' + simpleRandomLong));
      if (!fs.mkdirs(testTempDirPath)) {
        throw new IOException("Could not create " + testTempDirPath);
      }
      fs.deleteOnExit(testTempDirPath);
    }
    return testTempDirPath;
  }

  protected final File getTestTempDir() throws IOException {
    if (testTempDir == null) {
      String systemTmpDir = System.getProperty("java.io.tmpdir");
      long simpleRandomLong = (long) (Long.MAX_VALUE * Math.random());
      testTempDir = new File(systemTmpDir, "mahout-" + getClass().getSimpleName() + '-' + simpleRandomLong);
      if (!testTempDir.mkdir()) {
        throw new IOException("Could not create " + testTempDir);
      }
      testTempDir.deleteOnExit();
    }
    return testTempDir;
  }

  protected final File getTestTempFile(String name) throws IOException {
    return getTestTempFileOrDir(name, false);
  }

  protected final File getTestTempDir(String name) throws IOException {
    return getTestTempFileOrDir(name, true);
  }

  private File getTestTempFileOrDir(String name, boolean dir) throws IOException {
    File f = new File(getTestTempDir(), name);
    f.deleteOnExit();
    if (dir && !f.mkdirs()) {
      throw new IOException("Could not make directory " + f);
    }
    return f;
  }

  private static class DeletingVisitor implements FileFilter {
    @Override
    public boolean accept(File f) {
      if (!f.isFile()) {
        f.listFiles(this);
      }
      f.delete();
      return false;
    }
  }

  public List<String> readLines(String path) throws IOException {
    List<String> lines = new ArrayList<String>();
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(path)));
      String line;
      while ((line = reader.readLine()) != null) {
        lines.add(line);
      }
    } finally {
      Closeables.closeQuietly(reader);
    }
    return lines;
  }
}
