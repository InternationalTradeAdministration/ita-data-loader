package gov.ita.dataloader.ingest.translators;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.input.CharSequenceReader;

import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OtexaTradeBalanceCsvTranslator implements Translator {

  private ScientificNotationTranslator snt = new ScientificNotationTranslator();

  @Override
  public byte[] translate(byte[] bytes) {
    StringWriter stringWriter = new StringWriter();
    CSVPrinter csvPrinter;

    try {
      csvPrinter = new CSVPrinter(stringWriter, CSVFormat.DEFAULT
        .withHeader("country", "CTRYNUM", "ggrp", "YR", "MON", "HEADER_ID", "VAL"));

      String byteOrderMark = "\uFEFF";
      String byteString = new String(bytes);
      if (byteString.startsWith(byteOrderMark)) {
        byteString = byteString.substring(1);
      }

      Reader reader = new CharSequenceReader(byteString);
      CSVParser csvParser;
      csvParser = new CSVParser(
        reader,
        CSVFormat.DEFAULT.withFirstRecordAsHeader().withTrim().withNullString("").withIgnoreHeaderCase());

      Map<String, Integer> headers = csvParser.getHeaderMap();

      List<String> valueFields = headers.keySet().stream()
        .filter(header -> header.startsWith("I") || header.startsWith("E") || header.startsWith("YTD"))
        .collect(Collectors.toList());

      for (CSVRecord csvRecord : csvParser.getRecords()) {
        String country = csvRecord.get("country");
        String ctrynum = csvRecord.get("CTRYNUM");
        String ggrp = csvRecord.get("ggrp");
        String yr = csvRecord.get("YR");
        String mon = csvRecord.get("MON");

        for (String header : valueFields) {
          String val = csvRecord.get(header);
          if (val != null) {
            if (snt.isScientificNotation(val)) val = snt.translate(val);
            csvPrinter.printRecord(
              country, ctrynum, ggrp, yr, mon, header, val
            );
          }
        }
      }

      reader.close();
      csvPrinter.flush();

      return csvPrinter.getOut().toString().getBytes();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return null;
  }

  @Override
  public int pageSize() {
    return 25000;
  }

  @Override
  public TranslatorType type() {
    return TranslatorType.CSV;
  }
}
