package gov.ita.dataloader.ingest.translators;

import gov.ita.dataloader.TestHelpers;
import org.apache.commons.csv.CSVRecord;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static gov.ita.dataloader.TestHelpers.formattedResults;
import static org.junit.Assert.*;

public class OtexaAnnualFootwearCsvTranslatorTest {

  private final TestHelpers h = new TestHelpers();
  private List<CSVRecord> results;

  @Before
  public void setUp() {
    OtexaAnnualFootwearCsvTranslator otexaAnnualFootwearCsvTranslator = new OtexaAnnualFootwearCsvTranslator("AWESOME");
    byte[] translatedBytes = otexaAnnualFootwearCsvTranslator.translate(h.get("/fixtures/otexa/ANNUAL_FOOTWEAR.csv"));
    results = formattedResults(translatedBytes);
  }

  @Test
  public void translates_COUNTRY() {
    assertEquals("Greece", results.get(0).get("COUNTRY"));
  }

  @Test
  public void translates_CAT_ID() {
    assertEquals("100", results.get(0).get("CAT_ID"));
  }

  @Test
  public void translates_HTS() {
    assertEquals("6404118930", results.get(0).get("HTS"));
  }

  @Test
  public void translates_QUANTITY() {
    assertEquals("PRS", results.get(0).get("Quantity"));
  }

  @Test
  public void translates_NAICS() {
    assertEquals("316211", results.get(0).get("NAICS"));
  }

  @Test
  public void includes_DATA_TYPE() {
    assertEquals("AWESOME", results.get(0).get("DATA_TYPE"));
  }

  @Test
  public void translates_Y_Fields() {
    Map<String, String> expected = new HashMap<>();
    expected.put("Y_2001", "1");
    expected.put("Y_2002", "2");
    expected.put("Y_2003", "3");
    expected.put("Y_2004", "4");
    expected.put("Y_2005", "5");
    expected.put("Y_2006", "6");
    expected.put("Y_2007", "7");
    expected.put("Y_2008", "8");
    expected.put("Y_2009", "9");
    expected.put("Y_2010", "10");
    expected.put("Y_2011", "11");
    expected.put("Y_2012", "12");
    expected.put("Y_2013", "13");
    expected.put("Y_2014", "14");
    expected.put("Y_2015", "15");
    expected.put("Y_2016", "16");
    expected.put("Y_2017", "17");
    expected.put("Y_2018", "18");
    expected.put("Y_2019", "19");
    expected.put("Y_2019_YTD", "20");
    expected.put("Y_2020_YTD", "104916000000.000000"); //scientific notation

    //Only checking the first record
    List<CSVRecord> footwearValueRecords =
      results.stream().filter(r -> r.get("Country").equals("Greece")).collect(Collectors.toList());

    for (CSVRecord r : footwearValueRecords) {
      String header = r.get("HEADER_ID");
      String val = r.get("VAL");
      assertEquals("Expected " + header + " to have value of " + expected.get(val), expected.get(header), val);
    }
    assertEquals(21, footwearValueRecords.size());

  }
}
