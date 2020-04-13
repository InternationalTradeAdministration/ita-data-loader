package gov.ita.dataloader.cron_jobs;

import gov.ita.dataloader.TestHelpers;
import gov.ita.dataloader.ingest.configuration.BusinessUnitService;
import gov.ita.dataloader.storage.BlobMetaData;
import gov.ita.dataloader.storage.Storage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class DataRetentionPolicyTest {

  @Mock
  private Storage storage;
  @Mock
  private BusinessUnitService businessUnitService;

  TestHelpers h = new TestHelpers();

  @Before
  public void setUp() throws Exception {
    when(businessUnitService.getStorageContainers()).thenReturn(Arrays.asList("comic-container", "matrix-container"));

    List<BlobMetaData> comicPii = new ArrayList<>();
    comicPii.add(h.buildBlobMetaData("batman.txt", "comic-container", "snap-1", 2024, 11, false));
    comicPii.add(h.buildBlobMetaData("batman.txt", "comic-container", "snap-2", 2024, 12, false));
    comicPii.add(h.buildBlobMetaData("batman.txt", "comic-container", null, 2024, 12, false));

    List<BlobMetaData> matrixNonPii = new ArrayList<>();
    matrixNonPii.add(h.buildBlobMetaData("neo.txt", "matrix-container", "snap-1", 2020, 1, true));
    matrixNonPii.add(h.buildBlobMetaData("neo.txt", "matrix-container", "snap-2", 2020, 2, true));
    matrixNonPii.add(h.buildBlobMetaData("neo.txt", "matrix-container", null, 2020, 2, true));

    when(storage.getBlobMetadata("comic-container", true)).thenReturn(comicPii);
    when(storage.getBlobMetadata("matrix-container", true)).thenReturn(matrixNonPii);

    DataRetentionPolicy dataRetentionPolicy = new DataRetentionPolicy(storage, businessUnitService, h.datetimeOf(2026, 1));
    dataRetentionPolicy.purgeExpiredData();
  }

  @Test
  public void fileSnapshotsWithPiiOlderThan6YearsAreRemoved() throws Exception {
    verify(storage, times(1)).delete("matrix-container", "neo.txt", "snap-1");
    verify(storage, times(0)).delete("matrix-container", "neo.txt", "snap-2");
  }

  @Test
  public void fileSnapshotsWithOutPiiOlderThan13MonthsAreRemoved() throws Exception {
    verify(storage, times(1)).delete("comic-container", "batman.txt", "snap-1");
    verify(storage, times(0)).delete("comic-container", "batman.txt", "snap-2");
  }

}
