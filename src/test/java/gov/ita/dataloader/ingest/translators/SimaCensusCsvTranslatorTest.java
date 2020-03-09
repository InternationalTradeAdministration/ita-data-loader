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
  public void translates_CNTYDESC() {
    assertEquals("Canada", results.get(0).get("CNTYDESC"));
  }

  @Test
  public void translates_ldesccen36() {
    assertEquals("INGOTS AND STEEL FOR CASTINGS.........1A", results.get(0).get("ldesccen36"));
  }

  @Test
  public void translates_grade() {
    assertEquals("S", results.get(0).get("grade"));
  }

  @Test
  public void translates_VALUE() {
    assertEquals("0000918325", results.get(0).get("VALUE"));
  }

  @Test
  public void translates_UNKNOWN_FIELD_1() {
    assertEquals("0000000547.785", results.get(0).get("UNKNOWN_FIELD_1"));
  }

  @Test
  public void translates_UNKNOWN_FIELD_2() {
    assertEquals("000001676.43", results.get(0).get("UNKNOWN_FIELD_2"));
  }

  @Test
  public void translates_QTYMT() {
    assertEquals("0000000496.943", results.get(0).get("QTYMT"));
  }

  @Test
  public void translates_UnknownField3() {
    assertEquals("000001847.94", results.get(0).get("UNKNOWN_FIELD_3"));
  }

  @Test
  public void translates_Month() {
    assertEquals("12", results.get(0).get("SMONTH"));
  }

  @Test
  public void translates_Year() {
    assertEquals("2019", results.get(0).get("SYEAR"));
  }
}
