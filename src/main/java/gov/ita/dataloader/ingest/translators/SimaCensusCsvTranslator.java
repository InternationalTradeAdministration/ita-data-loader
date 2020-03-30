package gov.ita.dataloader.ingest.translators;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.io.StringWriter;

public class SimaCensusCsvTranslator implements Translator {

  @Override
  public byte[] translate(byte[] bytes) {
    StringWriter stringWriter = new StringWriter();
    CSVPrinter csvPrinter;

    try {
      csvPrinter = new CSVPrinter(stringWriter, CSVFormat.DEFAULT
        .withHeader(
          "COUNTRY",
          "CNTYDESC",
          "ldesccen36",
          "grade",
          "VALUE",
          "UNKNOWN_FIELD_1",
          "UNKNOWN_FIELD_2",
          "QTYMT",
          "UNKNOWN_FIELD_3",
          "SMONTH",
          "SYEAR"
        )
      );

      String[] lines = new String(bytes).split(System.getProperty("line.separator"));

      for (String line : lines) {
        String country = line.substring(0, 4);
        String countrydesc = line.substring(5, 35);
        String ldesccen36 = line.substring(36, 76);
        String grade = line.substring(77, 78);
        String value = line.substring(79, 89);
        String unknownField1 = line.substring(90, 104);
        String unknownField2 = line.substring(105, 117);
        String qtymt = line.substring(118, 132);
        String unknownField3 = line.substring(133, 145);
        String smonth = line.substring(146, 148);
        String syear = line.substring(149, 153);

        csvPrinter.printRecord(
          country,
          countrydesc,
          ldesccen36,
          grade,
          value,
          unknownField1,
          unknownField2,
          qtymt,
          unknownField3,
          smonth,
          syear
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
