package gov.ita.dataloader;

import gov.ita.dataloader.business_unit.BusinessUnitService;
import gov.ita.dataloader.storage.BlobMetaData;
import gov.ita.dataloader.storage.Storage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class DataRetentionPolicyTest {

  @Mock
  private Storage storage;
  @Mock
  private BusinessUnitService businessUnitService;

  @Before
  public void setUp() throws Exception {
    when(businessUnitService.getStorageContainers()).thenReturn(Arrays.asList("comic-container", "matrix-container"));

    List<BlobMetaData> comicPii = new ArrayList<>();
    comicPii.add(buildBlobMetaData("batman.txt", "comic-container", "snap-1", 2024, 11, false));
    comicPii.add(buildBlobMetaData("batman.txt", "comic-container", "snap-2", 2024, 12, false));
    comicPii.add(buildBlobMetaData("batman.txt", "comic-container", null, 2024, 12, false));

    List<BlobMetaData> matrixNonPii = new ArrayList<>();
    matrixNonPii.add(buildBlobMetaData("neo.txt", "matrix-container", "snap-1", 2020, 1, true));
    matrixNonPii.add(buildBlobMetaData("neo.txt", "matrix-container", "snap-2", 2020, 2, true));
    matrixNonPii.add(buildBlobMetaData("neo.txt", "matrix-container", null, 2020, 2, true));

    when(storage.getBlobMetadata("comic-container", true)).thenReturn(comicPii);
    when(storage.getBlobMetadata("matrix-container", true)).thenReturn(matrixNonPii);

    DataRetentionPolicy dataRetentionPolicy = new DataRetentionPolicy(storage, businessUnitService, datetimeOf(2026, 1));
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

  OffsetDateTime datetimeOf(Integer year, Integer month) {
    return OffsetDateTime.of(LocalDateTime.of(year, month, 1, 5, 45),
      ZoneOffset.ofHoursMinutes(0, 0));
  }

  BlobMetaData buildBlobMetaData(String fileName, String containerName, String snapshot, Integer year, Integer month, Boolean pii) {
    String url = (snapshot != null)
      ? String.format("https://fake.blob.core.windows.net/%s/%s?snapshot=%s", containerName, fileName, snapshot)
      : String.format("https://fake.blob.core.windows.net/%s/%s", containerName, fileName);
    return new BlobMetaData(fileName,
      snapshot,
      url,
      null,
      containerName,
      datetimeOf(year, month),
      piiMetadata(pii));
  }

  Map<String, String> piiMetadata(Boolean pii) {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("uploaded_at", pii.toString());
    metadata.put("pii", pii.toString());
    return metadata;
  }
}
