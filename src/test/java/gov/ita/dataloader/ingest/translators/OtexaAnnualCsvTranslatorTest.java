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
import static org.junit.Assert.assertEquals;

public class OtexaAnnualCsvTranslatorTest {

  private final TestHelpers h = new TestHelpers();
  private List<CSVRecord> results;

  @Before
  public void setUp() {
    OtexaAnnualCsvTranslator otexaAnnualCsvTranslator = new OtexaAnnualCsvTranslator();
    byte[] translatedBytes = otexaAnnualCsvTranslator.translate(h.get("/fixtures/otexa/ANNUAL.csv"));
    results = formattedResults(translatedBytes);
  }

  @Test
  public void translates_CNAME() {
    assertEquals("Greece", results.get(0).get("Country"));
  }

  @Test
  public void translates_CAT() {
    assertEquals("229", results.get(0).get("CAT_ID"));
  }

  @Test
  public void translates_SYEF() {
    assertEquals("13.59999999", results.get(0).get("SYEF"));
  }

  @Test
  public void translates_HTS() {
    assertEquals("3921121500", results.get(0).get("HTS"));
  }

  @Test
  public void translates_D_VAL_QTY() {
    Map<String, String> expected = new HashMap<>();
    expected.put("D1", "9363817.6");
    expected.put("D2", "11626762.4");
    expected.put("D3", "11704554.4");
    expected.put("D4", "7267976");
    expected.put("D5", "7531598.4");
    expected.put("D6", "11704554.4");
    expected.put("D7", "9627440");
    expected.put("D8", "12176855.2");
    expected.put("D9", "14632770.4");
    expected.put("D10", "15799718.4");
    expected.put("D11", "2181480.8");
    expected.put("D12", "2785905.6");
    expected.put("D13", "6510537.6");
    expected.put("D14", "4046193");
    expected.put("D15", "4026161");
    expected.put("D16", "4761244");
    expected.put("D17", "3172450");
    expected.put("D18", "2577020");
    expected.put("D19", "4761244");
    expected.put("D20", "3450763");
    expected.put("D21", "5078419");
    expected.put("D22", "5866447");
    expected.put("D23", "6210385");

    expected.put("DG28", "128632");
    expected.put("DG29", "581083");

    expected.put("QTY1989", "475959.1998");
//    expected.put("QTY1990", null); //removed nulls
    expected.put("QTY1991", "400084.7998");
    expected.put("QTY1992", "690349.5997");
    expected.put("QTY1993", "99334.39996");
    expected.put("QTY1994", "755330.3997");
    expected.put("QTY1995", "2208843.999");
    expected.put("QTY1996", "824377.5996");
    expected.put("QTY1997", "1646171.199");
    expected.put("QTY1998", "1421839.199");
    expected.put("QTY1999", "410624.7998");
    expected.put("QTY2000", "762524.7997");
    expected.put("QTY2001", "771323.9997");
    expected.put("QTY2002", "921916.7996");
    expected.put("QTY2003", "2505446.399");
    expected.put("QTY2004", "1014260.8");
    expected.put("QTY2005", "1748470.399");
    expected.put("QTY2006", "1337804.799");
    expected.put("QTY2007", "484703.9998");
    expected.put("QTY2008", "88943.99996");
    expected.put("QTY2009", "336871.9999");
    expected.put("QTY2010", "1725146.399");
    expected.put("QTY2011", "2804863.999");
    expected.put("QTY2012", "7324551.997");
    expected.put("QTY2013", "2536794.399");
    expected.put("QTY2014", "3349951.999");
    expected.put("QTY2015", "3190627.999");
    expected.put("QTY2016", "8280169.596");

    expected.put("VAL1989", "276320");
    expected.put("VAL1990", "635929");
    expected.put("VAL1991", "324919");
    expected.put("VAL1992", "471603");
    expected.put("VAL1993", "43244");
    expected.put("VAL1994", "451379");
    expected.put("VAL1995", "640019");
    expected.put("VAL1996", "255933");
    expected.put("VAL1997", "314077");
    expected.put("VAL1998", "332620");
    expected.put("VAL1999", "157472");
    expected.put("VAL2000", "259502");
    expected.put("VAL2001", "377169");
    expected.put("VAL2002", "329205");
    expected.put("VAL2003", "458335");
    expected.put("VAL2004", "275661");
    expected.put("VAL2005", "503170");
    expected.put("VAL2006", "372910");
    expected.put("VAL2007", "143697");
    expected.put("VAL2008", "45066");
    expected.put("VAL2009", "272591");
    expected.put("VAL2010", "1034683");
    expected.put("VAL2011", "2174955");
    expected.put("VAL2012", "2820779");
    expected.put("VAL2013", "1292766");
    expected.put("VAL2014", "1461669");
    expected.put("VAL2015", "2503140");
    expected.put("VAL2016", "2954847");

    expected.put("TSUSA", "104916000000.000000"); //scientific notation

    //Only checking the first record
    List<CSVRecord> otexaHtsRecords =
      results.stream().filter(r -> r.get("Country").equals("Greece")).collect(Collectors.toList());

    for (CSVRecord r : otexaHtsRecords) {
      String header = r.get("HEADER_ID");
      String val = r.get("VAL");
      assertEquals("Expected " + header + " to have value of " + expected.get(val), expected.get(header), val);
    }

    assertEquals(80, otexaHtsRecords.size());
  }

}