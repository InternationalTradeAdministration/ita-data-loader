package gov.ita.dataloader.ingest;

import gov.ita.dataloader.ingest.configuration.AutomatedIngestConfiguration;
import gov.ita.dataloader.security.AuthenticationFacade;
import gov.ita.dataloader.storage.Storage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class IngestControllerTest {

  @Mock
  private Storage storage;

  @Mock
  private AutomatedIngestProcessor automatedIngestProcessor;

  @Mock
  private AuthenticationFacade authenticationFacade;

  @Mock
  private ManualIngestProcessor manualIngestProcessor;

  private static byte[] DATALOADER_CONFIG = "dataloader blob".getBytes();
  private static byte[] SOME_BYTES = "some bytes".getBytes();

  @Before
  public void setUp() {
    when(authenticationFacade.getUserName()).thenReturn("TestUser@gmail.com");
  }

  @Test
  public void startsIngestProcess() {
    IngestController ingestController = new IngestController(storage, automatedIngestProcessor, authenticationFacade, manualIngestProcessor);
    ingestController.startIngestProcess("some-container", new AutomatedIngestConfiguration());

    verify(automatedIngestProcessor, times(1))
      .process(null, "some-container", "TestUser@gmail.com", 5000L);
  }

  @Test
  public void uploadedFileIsSavedAndPassedToTranslationProcessor() throws IOException {
    IngestController ingestController = new IngestController(storage, automatedIngestProcessor, authenticationFacade, manualIngestProcessor);
    MultipartFile multipartFile = mock(MultipartFile.class);
    when(multipartFile.getBytes()).thenReturn(SOME_BYTES);
    when(multipartFile.getOriginalFilename()).thenReturn("OG File Name.csv");

    ingestController.saveFile(multipartFile, "cool-container", false);

    verify(storage, times(1)).save("OG File Name.csv", SOME_BYTES, "TestUser@gmail.com", "cool-container", true, false);
    verify(storage, times(1)).makeSnapshot("cool-container", "OG File Name.csv");
    verify(manualIngestProcessor, times(1))
      .process("cool-container", "OG File Name.csv", SOME_BYTES);
  }

  @Test
  public void uploadedFilePassedThroughTranslationProcessor() throws IOException {
    IngestController ingestController = new IngestController(storage, null, authenticationFacade, manualIngestProcessor);
    MultipartFile multipartFile = mock(MultipartFile.class);
    when(multipartFile.getOriginalFilename()).thenReturn("OG File Name.csv");
    when(multipartFile.getBytes()).thenReturn(SOME_BYTES);

    ingestController.saveFile(multipartFile, "cool-container", false);

    verify(manualIngestProcessor, times(1))
      .process("cool-container", "OG File Name.csv", SOME_BYTES);
  }

}
