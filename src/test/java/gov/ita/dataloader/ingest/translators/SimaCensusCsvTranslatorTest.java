package gov.ita.dataloader.ingest.translators;

import gov.ita.dataloader.TestHelpers;
import org.apache.commons.csv.CSVRecord;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static gov.ita.dataloader.TestHelpers.formattedResults;
import static org.junit.Assert.assertEquals;

public class SimaCensusCsvTranslatorTest {

  private TestHelpers h = new TestHelpers();
  private List<CSVRecord> results;

  @Before
  public void setUp() {
    SimaCensusCsvTranslator simaCensusCsvTranslator = new SimaCensusCsvTranslator();
    byte[] translatedBytes = simaCensusCsvTranslator.translate(h.get("/fixtures/sima/Dec2019P.txt"));
    results = formattedResults(translatedBytes);
  }

  @Test
  public void translates_COUNTRY() {
    assertEquals("1220", results.get(0).get("COUNTRY"));
  }

  @Test
  public void translates_CTRY_DESC() {
    assertEquals("Canada", results.get(0).get("CTRY_DESC"));
  }

  @Test
  public void translates_COMM_DESC() {
    assertEquals("INGOTS AND STEEL FOR CASTINGS.........1A", results.get(0).get("COMM_DESC"));
  }

  @Test
  public void translates_STEEL_TYPE() {
    assertEquals("S", results.get(0).get("STEEL_TYPE"));
  }

  @Test
  public void translates_VALUE() {
    assertEquals("0000918325", results.get(0).get("VALUE"));
  }

  @Test
  public void translates_NETTON() {
    assertEquals("0000000547.785", results.get(0).get("NETTON"));
  }

  @Test
  public void translates_AVGNETPR() {
    assertEquals("000001676.43", results.get(0).get("AVGNETPR"));
  }

  @Test
  public void translates_METRICTON() {
    assertEquals("0000000496.943", results.get(0).get("METRICTON"));
  }

  @Test
  public void translates_AVGMETPR() {
    assertEquals("000001847.94", results.get(0).get("AVGMETPR"));
  }

  @Test
  public void translates_STAT_MO() {
    assertEquals("12", results.get(0).get("STAT_MO"));
  }

  @Test
  public void translates_STAT_YEAR() {
    assertEquals("2019", results.get(0).get("STAT_YEAR"));
  }
}
