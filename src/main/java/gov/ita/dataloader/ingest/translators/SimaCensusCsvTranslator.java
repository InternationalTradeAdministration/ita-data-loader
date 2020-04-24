package gov.ita.dataloader.ingest.translators;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.io.StringWriter;

public class SimaCensusCsvTranslator implements Translator {

  /**
   * Used the following document as a reference: //https://www.census.gov/foreign-trade/structures/stlstruc.txt
   */

  @Override
  public byte[] translate(byte[] bytes) {
    StringWriter stringWriter = new StringWriter();
    CSVPrinter csvPrinter;

    try {
      csvPrinter = new CSVPrinter(stringWriter, CSVFormat.DEFAULT
        .withHeader(
          "COUNTRY",
          "CTRY_DESC",
          "COMM_DESC",
          "STEEL_TYPE",
          "VALUE",
          "NETTON",
          "AVGNETPR",
          "METRICTON",
          "AVGMETPR",
          "STAT_MO",
          "STAT_YEAR"
        )
      );

      String[] lines = new String(bytes).split(System.getProperty("line.separator"));

      for (String line : lines) {
        String countryId = line.substring(0, 4);
        String countryDescription = line.substring(5, 35);
        String commodityDescription = line.substring(36, 76);
        String steelType = line.substring(77, 78);
        String value = line.substring(79, 89);
        String netton = line.substring(90, 104);
        String avgnetpr = line.substring(105, 117);
        String metricton = line.substring(118, 132);
        String avgmetpr = line.substring(133, 145);
        String month = line.substring(146, 148);
        String year = line.substring(149, 153);

        csvPrinter.printRecord(
          countryId,
          countryDescription,
          commodityDescription,
          steelType,
          value,
          netton,
          avgnetpr,
          metricton,
          avgmetpr,
          month,
          year
        );
      }

      csvPrinter.flush();

      return csvPrinter.getOut().toString().getBytes();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return null;
  }

  @Override
  public int pageSize() {
    return -1;
  }

  @Override
  public TranslatorType type() {
    return TranslatorType.CSV;
  }
}
