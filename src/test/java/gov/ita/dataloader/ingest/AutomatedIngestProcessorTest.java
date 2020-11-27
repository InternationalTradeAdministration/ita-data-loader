package gov.ita.dataloader.ingest;

import gov.ita.dataloader.HttpHelper;
import gov.ita.dataloader.data_factory.DataFactoryGateway;
import gov.ita.dataloader.ingest.configuration.DataSetConfig;
import gov.ita.dataloader.ingest.configuration.ReplaceValue;
import gov.ita.dataloader.ingest.configuration.ZipFileConfig;
import gov.ita.dataloader.storage.Storage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class AutomatedIngestProcessorTest {

  @Mock
  private ZipFileExtractor zipFileExtractor;

  @Mock
  private HttpHelper httpHelper;

  @Mock
  private TranslationProcessor translationProcessor;

  @Mock
  private Storage storage;

  @Mock
  private DataFactoryGateway datafactoryGateway;

  private List<DataSetConfig> dataSetConfigs;

  private byte[] RAD_BYTES = "rad bytes".getBytes();
  private byte[] VERY_RAD_BYTES = "very rad bytes".getBytes();
  private byte[] REALLY_RAD_BYTES = "really rad bytes".getBytes();
  private byte[] ZIP_FILE_BYTES = "zip file bytes".getBytes();

  @Before
  public void setUp() throws Exception {
    when(httpHelper.getBytes("http://cool.io")).thenReturn(RAD_BYTES);
    when(httpHelper.getBytes("http://very-cool.io")).thenReturn(VERY_RAD_BYTES);
    when(httpHelper.getBytes("http://really-cool.io")).thenReturn(REALLY_RAD_BYTES);
    when(httpHelper.getBytes("http://vangos-zip.io")).thenReturn(ZIP_FILE_BYTES);
    dataSetConfigs = new ArrayList<>();
  }

  @Test
  public void processDataSetConfig() {
    dataSetConfigs.add(new DataSetConfig("http://cool.io", true, "rad.csv", null, null));
    dataSetConfigs.add(new DataSetConfig("http://very-cool.io", true, "very-rad.csv", null, null));
    dataSetConfigs.add(new DataSetConfig("http://really-cool.io", true, "really-rad.csv", null, null));

    AutomatedIngestProcessor automatedIngestProcessor =
      new AutomatedIngestProcessor(null, httpHelper, translationProcessor, storage, datafactoryGateway);
    automatedIngestProcessor.process(dataSetConfigs, "a-container", "TestUser@gmail.com", 0);

    verify(storage, times(1))
      .save("rad.csv", RAD_BYTES, "TestUser@gmail.com", "a-container", false, false);
    verify(storage, times(1))
      .save("very-rad.csv", VERY_RAD_BYTES, "TestUser@gmail.com", "a-container", false, false);
    verify(storage, times(1))
      .save("really-rad.csv", REALLY_RAD_BYTES, "TestUser@gmail.com", "a-container", false, false);

    verify(storage, times(1))
      .makeSnapshot("a-container", "rad.csv");
    verify(storage, times(1))
      .makeSnapshot("a-container", "very-rad.csv");
    verify(storage, times(1))
      .makeSnapshot("a-container", "really-rad.csv");

    verify(translationProcessor, times(1))
      .process("a-container", "rad.csv", RAD_BYTES);
    verify(translationProcessor, times(1))
      .process("a-container", "very-rad.csv", VERY_RAD_BYTES);
    verify(translationProcessor, times(1))
      .process("a-container", "really-rad.csv", REALLY_RAD_BYTES);
  }

  @Test
  public void processDataSetConfigWithReplaceValues() throws Exception {
    List<ReplaceValue> replaceValues = new ArrayList<>();
    replaceValues.add(new ReplaceValue("baseball", "football"));
    dataSetConfigs.add(new DataSetConfig("http://vango.io", true, "vangos.csv", replaceValues, null));

    when(httpHelper.getBytes("http://vango.io")).thenReturn("The best sport is baseball!".getBytes());

    AutomatedIngestProcessor automatedIngestProcessor =
      new AutomatedIngestProcessor(null, httpHelper, translationProcessor, storage, datafactoryGateway);
    automatedIngestProcessor.process(dataSetConfigs, "a-container", "TestUser@gmail.com", 0);

    verify(storage, times(1))
      .save("vangos.csv", "The best sport is football!".getBytes(), "TestUser@gmail.com", "a-container", false, false);
    verify(storage, times(1))
      .makeSnapshot("a-container", "vangos.csv");
    verify(translationProcessor, times(1))
      .process("a-container", "vangos.csv", "The best sport is football!".getBytes());
  }

  @Test
  public void processDataSetConfigWithZipConfig() throws Exception {
    List<ReplaceValue> replaceValues = new ArrayList<>();
    replaceValues.add(new ReplaceValue("swimming", "hiking"));
    replaceValues.add(new ReplaceValue("biking", "climbing"));

    List<ZipFileConfig> zipFileConfigs = new ArrayList<>();
    zipFileConfigs.add(new ZipFileConfig("Hobbies_1342.csv", "Hobbies_A.csv", null, replaceValues));
    zipFileConfigs.add(new ZipFileConfig("Hobbies_5674.csv", "Hobbies_B.csv", null, replaceValues));

    dataSetConfigs.add(new DataSetConfig("http://vangos-zip.io", true, "vangos.zip", null, zipFileConfigs));

    Map<String, ByteArrayOutputStream> zipFileContents = new HashMap<>();
    zipFileContents.put("Hobbies_1342.csv", convert("My favorite hobby is swimming!"));
    zipFileContents.put("Hobbies_5674.csv", convert("My favorite hobby is biking!"));

    when(zipFileExtractor.extract(ZIP_FILE_BYTES)).thenReturn(zipFileContents);

    AutomatedIngestProcessor automatedIngestProcessor = new AutomatedIngestProcessor(zipFileExtractor, httpHelper, translationProcessor, storage, datafactoryGateway);
    automatedIngestProcessor.process(dataSetConfigs, "a-container", "TestUser@gmail.com", 0);

    verify(translationProcessor, times(1))
      .process("a-container", "vangos.zip", ZIP_FILE_BYTES);
    verify(translationProcessor, times(1))
      .process("a-container", "Hobbies_A.csv", "My favorite hobby is hiking!".getBytes());
    verify(translationProcessor, times(1))
      .process("a-container", "Hobbies_B.csv", "My favorite hobby is climbing!".getBytes());
  }

  @Test
  public void processDataSetConfigWithSkipLinesInZipConfig() throws Exception {
    Integer skipLineCount = 2;

    List<ZipFileConfig> zipFileConfigs = new ArrayList<>();
    zipFileConfigs.add(new ZipFileConfig("Hobbies_skip.csv", "Skipped_Hobbies.csv", skipLineCount, null));
    dataSetConfigs.add(new DataSetConfig("http://vangos-zip.io", true, "vangos.zip", null, zipFileConfigs));

    when(httpHelper.getBytes("http://vangos-zip.io")).thenReturn(ZIP_FILE_BYTES);

    Map<String, ByteArrayOutputStream> zipFileContents = new HashMap<>();
    zipFileContents.put("Hobbies_skip.csv", convert("zip file bytes \n more bytes \n and more \n never ending bytes"));

    when(zipFileExtractor.extract(ZIP_FILE_BYTES)).thenReturn(zipFileContents);

    AutomatedIngestProcessor automatedIngestProcessor = new AutomatedIngestProcessor(zipFileExtractor, httpHelper, translationProcessor, storage, datafactoryGateway);
    automatedIngestProcessor.process(dataSetConfigs, "a-container", "TestUser@gmail.com", 0);

    verify(translationProcessor, times(1))
      .process("a-container", "vangos.zip", ZIP_FILE_BYTES);
    verify(translationProcessor, times(1))
      .process("a-container", "Skipped_Hobbies.csv", " and more \n never ending bytes\n".getBytes());
  }

  @Test
  public void sendFilesThroughIngestTranslationProcessor() {
    dataSetConfigs.add(new DataSetConfig("http://cool.io", true, "rad.csv", null, null));

    AutomatedIngestProcessor automatedIngestProcessor =
      new AutomatedIngestProcessor(zipFileExtractor, httpHelper, translationProcessor, storage, datafactoryGateway);
    automatedIngestProcessor.process(dataSetConfigs, "a-container", "TestUser@gmail.com", 0);

    verify(translationProcessor, times(1))
      .process("a-container", "rad.csv", RAD_BYTES);
    verify(translationProcessor, times(1))
      .process("a-container", "rad.csv", RAD_BYTES);
  }

  @Test
  public void triggersPipelineRun() {
    AutomatedIngestProcessor automatedIngestProcessor =
      new AutomatedIngestProcessor(zipFileExtractor, httpHelper, translationProcessor, storage, datafactoryGateway);
    automatedIngestProcessor.process(dataSetConfigs, "a-container", "TestUser@gmail.com", 0);
    verify(datafactoryGateway, times(1)).runPipeline("a-container", "");
  }

  private ByteArrayOutputStream convert(String s) {
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    for (int i = 0; i < s.length(); ++i)
      outStream.write(s.charAt(i));

    return outStream;
  }
}