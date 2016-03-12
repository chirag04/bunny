package org.rabix.bindings.protocol.draft2.bean;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.rabix.bindings.BindingException;
import org.rabix.bindings.Bindings;
import org.rabix.bindings.BindingsFactory;
import org.rabix.bindings.model.Executable;
import org.rabix.bindings.model.dag.DAGNode;
import org.rabix.common.helper.ResourceHelper;
import org.rabix.common.json.BeanSerializer;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = { "functional" })
public class Draft2CommandLineToolTest {

  @Test
  public void testBwaMemCmdLine() throws IOException {
    String inputJson = ResourceHelper.readResource(this.getClass(), "bwa-mem-job.json");

    Draft2Job job = BeanSerializer.deserialize(inputJson, Draft2Job.class);

    List<Object> expectedList = new LinkedList<Object>();
    expectedList.add("bwa");
    expectedList.add("mem");
    expectedList.add("-t");
    expectedList.add(3);
    expectedList.add("-m");
    expectedList.add(3);
    expectedList.add("-I");
    expectedList.add("1,2,3,4");
    expectedList.add("rabix/tests/test-files/chr20.fa.tmp");
    expectedList.add("rabix/tests/test-files/example_human_Illumina.pe_1.fastq");
    expectedList.add("rabix/tests/test-files/example_human_Illumina.pe_2.fastq");

    List<?> resultList;
    try {
      DAGNode dagNode = new DAGNode("id", null, null, null, job.getApp());
      Executable executable = new Executable("id", "id", dagNode, null, job.getInputs(), null, null);
      Bindings bindings = BindingsFactory.create(executable);
      resultList = bindings.buildCommandLineParts(executable);

      Assert.assertNotNull(resultList);
      Assert.assertEquals(resultList.size(), expectedList.size());
      Assert.assertEquals(resultList, expectedList);
    } catch (BindingException e) {
      Assert.fail(e.getMessage());
    }
  }
  
  @Test
  public void testExpressionTool() throws IOException {
    String inputJson = ResourceHelper.readResource(this.getClass(), "expression-job.json");

    Draft2Job job = BeanSerializer.deserialize(inputJson, Draft2Job.class);

    List<?> resultList;
    try {
      DAGNode dagNode = new DAGNode("id", null, null, null, job.getApp());
      Executable executable = new Executable("id", "id", dagNode, null, job.getInputs(), null, null);
      Bindings bindings = BindingsFactory.create(executable);
      resultList = bindings.buildCommandLineParts(executable);

      Assert.assertNull(resultList);
    } catch (BindingException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testPicardGather() throws IOException {
    String inputJson = ResourceHelper.readResource(this.getClass(), "picard-gather-job.json");

    Draft2Job job = BeanSerializer.deserialize(inputJson, Draft2Job.class);

    List<Object> expectedList = new LinkedList<Object>();
    expectedList.add("java");
    expectedList.add("-jar");
    expectedList.add("/picard-tools-1.126/picard.jar");
    expectedList.add("GatherBamFiles");
    expectedList.add("O=gathered.bam");
    expectedList.add("I=/home/temp1");
    expectedList.add("I=/home/temp2");

    List<?> resultList;
    try {
      DAGNode dagNode = new DAGNode("id", null, null, null, job.getApp());
      Executable executable = new Executable("id", "id", dagNode, null, job.getInputs(), null, null);
      Bindings bindings = BindingsFactory.create(executable);
      resultList = bindings.buildCommandLineParts(executable);

      Assert.assertNotNull(resultList);
      Assert.assertEquals(resultList.size(), expectedList.size());
      Assert.assertEquals(resultList, expectedList);
    } catch (BindingException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testBwaMemCmdLineStr() throws IOException {
    String inputJson = ResourceHelper.readResource(this.getClass(), "bwa-mem-job.json");

    Draft2Job job = BeanSerializer.deserialize(inputJson, Draft2Job.class);

    try {
      DAGNode dagNode = new DAGNode("id", null, null, null, job.getApp());
      Executable executable = new Executable("id", "id", dagNode, null, job.getInputs(), null, null);
      Bindings bindings = BindingsFactory.create(executable);
      String commandLine = bindings.buildCommandLine(executable);

      Assert.assertEquals(commandLine, "bwa mem -t 3 -m 3 -I 1,2,3,4 rabix/tests/test-files/chr20.fa.tmp rabix/tests/test-files/example_human_Illumina.pe_1.fastq rabix/tests/test-files/example_human_Illumina.pe_2.fastq < input.txt > output.sam");
    } catch (BindingException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test(enabled = true)
  public void testTMapCmdLine() throws IOException {
    String inputJson = ResourceHelper.readResource(this.getClass(), "tmap-job.json");

    Draft2Job job = BeanSerializer.deserialize(inputJson, Draft2Job.class);

    List<Object> expectedList = new LinkedList<Object>();
    expectedList.add("tmap");
    expectedList.add("mapall");
    expectedList.add("stage");
    expectedList.add(1);
    expectedList.add("map1");
    expectedList.add("--min-seq-length");
    expectedList.add(20);
    expectedList.add("map2");
    expectedList.add("--min-seq-length");
    expectedList.add(20);
    expectedList.add("stage");
    expectedList.add(2);
    expectedList.add("map1");
    expectedList.add("--min-seq-length");
    expectedList.add(10);
    expectedList.add("--max-seq-length");
    expectedList.add(20);
    expectedList.add("--seed-length");
    expectedList.add(16);
    expectedList.add("map2");
    expectedList.add("--min-seq-length");
    expectedList.add(10);
    expectedList.add("--max-seq-length");
    expectedList.add(20);

    List<?> resultList;
    try {
      DAGNode dagNode = new DAGNode("id", null, null, null, job.getApp());
      Executable executable = new Executable("id", "id", dagNode, null, job.getInputs(), null, null);
      Bindings bindings = BindingsFactory.create(executable);
      resultList = bindings.buildCommandLineParts(executable);

      Assert.assertNotNull(resultList);
      Assert.assertEquals(resultList.size(), expectedList.size());
      Assert.assertEquals(resultList, expectedList);
    } catch (BindingException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testCleanSamCmdLine() throws IOException {
    String inputJson = ResourceHelper.readResource(this.getClass(), "cleansam.json");

    Draft2Job job = BeanSerializer.deserialize(inputJson, Draft2Job.class);

    List<Object> expectedList = new LinkedList<Object>();
    expectedList.add("java");
    expectedList.add("-jar");
    expectedList.add("/picard-tools-1.126/picard.jar");
    expectedList.add("CleanSam");
    expectedList.add("O=input.cleaned.bam");
    expectedList.add("VALIDATION_STRINGENCY=SILENT");
    expectedList.add("CREATE_INDEX=True");
    expectedList.add("VERBOSITY=INFO");

    List<?> resultList;
    try {
      DAGNode dagNode = new DAGNode("id", null, null, null, job.getApp());
      Executable executable = new Executable("id", "id", dagNode, null, job.getInputs(), null, null);
      Bindings bindings = BindingsFactory.create(executable);
      resultList = bindings.buildCommandLineParts(executable);

      Assert.assertNotNull(resultList);
      Assert.assertEquals(resultList.size(), expectedList.size());
      Assert.assertEquals(resultList, expectedList);
    } catch (BindingException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testSortSamCmdLine() throws IOException {
    String inputJson = ResourceHelper.readResource(this.getClass(), "sortsam.json");

    Draft2Job job = BeanSerializer.deserialize(inputJson, Draft2Job.class);

    List<Object> expectedList = new LinkedList<Object>();
    expectedList.add("java -jar /picard-tools-1.126/picard.jar SortSam");
    expectedList.add("OUTPUT=input.sorted.bam");
    expectedList.add("QUIET=True");
    expectedList.add("VALIDATION_STRINGENCY=SILENT");
    expectedList.add("COMPRESSION_LEVEL=50");
    expectedList.add("SO=unsorted");
    expectedList.add("CREATE_INDEX=True");

    List<?> resultList;
    try {
      DAGNode dagNode = new DAGNode("id", null, null, null, job.getApp());
      Executable executable = new Executable("id", "id", dagNode, null, job.getInputs(), null, null);
      Bindings bindings = BindingsFactory.create(executable);
      resultList = bindings.buildCommandLineParts(executable);

      Assert.assertNotNull(resultList);
      Assert.assertEquals(resultList.size(), expectedList.size());
      Assert.assertEquals(resultList, expectedList);
    } catch (BindingException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testMarkDuplicatesCmdLine() throws IOException {
    String inputJson = ResourceHelper.readResource(this.getClass(), "mark-duplicates.json");

    Draft2Job job = BeanSerializer.deserialize(inputJson, Draft2Job.class);

    List<Object> expectedList = new LinkedList<Object>();
    expectedList.add("java -jar /picard-tools-1.126/picard.jar MarkDuplicates");
    expectedList.add("M=input.metrics");
    expectedList.add("O=input.deduped.bam");
    expectedList.add("AS=False");
    expectedList.add("CREATE_INDEX=True");
    expectedList.add("READ_NAME_REGEX=something");
    expectedList.add("SORTING_COLLECTION_SIZE_RATIO=50");
    expectedList.add("MAX_FILE_HANDLES=max_file_handles_something");
    expectedList.add("MAX_SEQS=20");
    expectedList.add("COMMENT=some_comment");
    expectedList.add("PG_NAME=group_name");

    List<?> resultList;
    try {
      DAGNode dagNode = new DAGNode("id", null, null, null, job.getApp());
      Executable executable = new Executable("id", "id", dagNode, null, job.getInputs(), null, null);
      Bindings bindings = BindingsFactory.create(executable);
      resultList = bindings.buildCommandLineParts(executable);

      Assert.assertNotNull(resultList);
      Assert.assertEquals(resultList.size(), expectedList.size());
      Assert.assertEquals(resultList, expectedList);
    } catch (BindingException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testSha1CmdLine() throws IOException {
    String inputJson = ResourceHelper.readResource(this.getClass(), "sha1.json");

    Draft2Job job = BeanSerializer.deserialize(inputJson, Draft2Job.class);

    List<Object> expectedList = new LinkedList<Object>();
    expectedList.add("/usr/local/bin/sha1.py");
    expectedList.add("--i");
    expectedList.add("rabix/tests/test-files/chr20.fa");
    expectedList.add("--rbx");

    List<?> resultList;
    try {
      DAGNode dagNode = new DAGNode("id", null, null, null, job.getApp());
      Executable executable = new Executable("id", "id", dagNode, null, job.getInputs(), null, null);
      Bindings bindings = BindingsFactory.create(executable);
      resultList = bindings.buildCommandLineParts(executable);

      Assert.assertNotNull(resultList);
      Assert.assertEquals(resultList.size(), expectedList.size());
      Assert.assertEquals(resultList, expectedList);
    } catch (BindingException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testSha1ObjCmdLine() throws IOException {
    String inputJson = ResourceHelper.readResource(this.getClass(), "sha1-obj.json");

    Draft2Job job = BeanSerializer.deserialize(inputJson, Draft2Job.class);

    List<Object> expectedList = new LinkedList<Object>();
    expectedList.add("/usr/local/bin/sha1.py");
    expectedList.add("--i");
    expectedList.add("rabix/tests/test-files/chr20.fa");
    expectedList.add("--rbx");

    List<?> resultList;
    try {
      DAGNode dagNode = new DAGNode("id", null, null, null, job.getApp());
      Executable executable = new Executable("id", "id", dagNode, null, job.getInputs(), null, null);
      Bindings bindings = BindingsFactory.create(executable);
      resultList = bindings.buildCommandLineParts(executable);

      Assert.assertNotNull(resultList);
      Assert.assertEquals(resultList.size(), expectedList.size());
      Assert.assertEquals(resultList, expectedList);
    } catch (BindingException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testSha1ArrLine() throws IOException {
    String inputJson = ResourceHelper.readResource(this.getClass(), "sha1-arr.json");

    Draft2Job job = BeanSerializer.deserialize(inputJson, Draft2Job.class);

    List<Object> expectedList = new LinkedList<Object>();
    expectedList.add("/usr/local/bin/sha1.py");
    expectedList.add("--i");
    expectedList.add("rabix/tests/test-files/chr20.fa");
    expectedList.add("--rbx");

    List<?> resultList;
    try {
      DAGNode dagNode = new DAGNode("id", null, null, null, job.getApp());
      Executable executable = new Executable("id", "id", dagNode, null, job.getInputs(), null, null);
      Bindings bindings = BindingsFactory.create(executable);
      resultList = bindings.buildCommandLineParts(executable);

      Assert.assertNotNull(resultList);
      Assert.assertEquals(resultList.size(), expectedList.size());
      Assert.assertEquals(resultList, expectedList);
    } catch (BindingException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testSha1ObjArrLine() throws IOException {
    String inputJson = ResourceHelper.readResource(this.getClass(), "sha1-arr-obj.json");

    Draft2Job job = BeanSerializer.deserialize(inputJson, Draft2Job.class);

    List<Object> expectedList = new LinkedList<Object>();
    expectedList.add("/usr/local/bin/sha1.py");
    expectedList.add("--i");
    expectedList.add("rabix/tests/test-files/chr20.fa");
    expectedList.add("--rbx");

    List<?> resultList;
    try {
      DAGNode dagNode = new DAGNode("id", null, null, null, job.getApp());
      Executable executable = new Executable("id", "id", dagNode, null, job.getInputs(), null, null);
      Bindings bindings = BindingsFactory.create(executable);
      resultList = bindings.buildCommandLineParts(executable);

      Assert.assertNotNull(resultList);
      Assert.assertEquals(resultList.size(), expectedList.size());
      Assert.assertEquals(resultList, expectedList);
    } catch (BindingException e) {
      Assert.fail(e.getMessage());
    }
  }
}