package gov.ita.dataloader;

import gov.ita.dataloader.storage.BlobMetaData;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.CharSequenceReader;

import java.io.IOException;
import java.io.Reader;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestHelpers {

  public byte[] get(String path) {
    try {
      return IOUtils.toString(
        this.getClass().getResourceAsStream(path)
      ).getBytes();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  public static List<CSVRecord> formattedResults(byte[] translatedBytes) {
    Reader reader = new CharSequenceReader(new String(translatedBytes));
    CSVParser csvParser;
    try {
      csvParser = new CSVParser(
        reader,
        CSVFormat.DEFAULT.withFirstRecordAsHeader().withTrim().withNullString("").withIgnoreHeaderCase());
      return csvParser.getRecords();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  public BlobMetaData buildBlobMetaData(String fileName, String containerName, String snapshot, Integer year, Integer month, Integer day, Boolean pii) {
    String url = (snapshot != null)
      ? String.format("https://fake.blob.core.windows.net/%s/%s?snapshot=%s", containerName, fileName, snapshot)
      : String.format("https://fake.blob.core.windows.net/%s/%s", containerName, fileName);
    return new BlobMetaData(fileName,
      snapshot,
      url,
      null,
      containerName,
      datetimeOf(year, month, day),
      piiMetadata(pii));
  }

  public OffsetDateTime datetimeOf(Integer year, Integer month, Integer day) {
    return OffsetDateTime.of(LocalDateTime.of(year, month, day, 5, 45),
      ZoneOffset.ofHoursMinutes(0, 0));
  }

  public BlobMetaData buildBlobMetaData(String fileName, String containerName, String snapshot, Integer year, Integer month, Boolean pii) {
    return buildBlobMetaData(fileName, containerName, snapshot, year, month, 1, pii);
  }

  public OffsetDateTime datetimeOf(Integer year, Integer month) {
    return datetimeOf(year, month, 1);
  }

  Map<String, String> piiMetadata(Boolean pii) {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("uploaded_at", pii.toString());
    metadata.put("pii", pii.toString());
    return metadata;
  }
}
