package it.deloitte.postrxade.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

public class TransatoAnafrageFilter {

    public static void main(String[] args) {
        System.out.println("Using default hardcoded paths. Please adjust as needed.");

        String transatoFile = "src/main/resources/TRXPOSADE_32875_TRANSATOPOS_202508_20251121143100.txt";
        String anafrageFile = "src/main/resources/Original Anagrafe.txt";
        String outputFile = "src/main/resources/TRXPOSADE_32875_ANAGRAFEPOS_202508_20251121143100.txt";

        try {
            filterAnafrageByTransatoSubstrings(transatoFile, anafrageFile, outputFile);
            System.out.println("Filtering completed successfully!");
            System.out.println("Output file: " + outputFile);
        } catch (IOException e) {
            System.err.println("Error processing files: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Extracts substrings from transato file and uses them to filter anafrage file.
     */
    public static void filterAnafrageByTransatoSubstrings(String transatoFilePath,
                                                          String anafrageFilePath,
                                                          String outputFilePath) throws IOException {

        Set<String> merchantValues = new HashSet<>();

        try (BufferedReader transatoReader = new BufferedReader(
                new InputStreamReader(new FileInputStream(transatoFilePath), StandardCharsets.UTF_8))) {

            String line;
            int lineNumber = 0;

            while ((line = transatoReader.readLine()) != null) {
                lineNumber++;

                if (line.length() >= 42) {
                    String substring = line.substring(42, 92).trim();
                    if (!substring.isEmpty()) {
                        merchantValues.add(substring);
                    }
                }

                if (lineNumber % 5000 == 0) {
                    System.out.println("TRANSATO - processed " + lineNumber + " lines...");
                }
            }

            System.out.println("TRANSATO - total lines processed: " + lineNumber);
            System.out.println("TRANSATO - unique substrings collected: " + merchantValues.size());
        }

        // 2) Filter ANAFRAGE file using the HashSet
        try (BufferedReader anafrageReader = new BufferedReader(
                new InputStreamReader(new FileInputStream(anafrageFilePath), StandardCharsets.UTF_8));
             BufferedWriter writer = new BufferedWriter(
                     new OutputStreamWriter(new FileOutputStream(outputFilePath), StandardCharsets.UTF_8))) {

            String line;
            int lineNumber = 0;
            int matchedLines = 0;

            while ((line = anafrageReader.readLine()) != null) {
                lineNumber++;

                String trim = line.substring(12, 42).trim();
                if (merchantValues.contains(trim)) {
                    writer.write(line);
                    writer.newLine();
                    matchedLines++;
                }

                if (lineNumber % 5000 == 0) {
                    System.out.println("ANAFRAGE - processed " + lineNumber + " lines...");
                }
            }

            System.out.println("ANAFRAGE - total lines processed: " + lineNumber);
            System.out.println("ANAFRAGE - lines matching HashSet values: " + matchedLines);
        }
    }
}


