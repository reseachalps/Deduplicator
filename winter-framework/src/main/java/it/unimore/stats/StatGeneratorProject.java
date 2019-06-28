package it.unimore.stats;

import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.HashedDataSet;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.CSVRecordReader;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Record;
import it.unimore.deduplication.model.Organization;
import it.unimore.deduplication.model.Project;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class StatGeneratorProject {


    public static void main(String arg[]) {

        DataSet<Record, Attribute> openaire = new HashedDataSet<>();
        DataSet<Record, Attribute> arianna = new HashedDataSet<>();

        DataSet<Record, Attribute> scanr = new HashedDataSet<>();
        DataSet<Record, Attribute> questio = new HashedDataSet<>();
        DataSet<Record, Attribute> p3 = new HashedDataSet<>();
        DataSet<Record, Attribute> all = new HashedDataSet<>();


//        String basePath = arg
//                [0];

        String basePath = "/Users/paolosottovia/Downloads/csv_data_25_07_2018/";
        ///Users/paolosottovia/Downloads/new_exported_data/



        File ariannaFile = new File(basePath + "projects_Arianna - Anagrafe Nazionale delle Ricerche.csv");

        File openaireFile = new File(basePath + "projects_OpenAire.csv");


        File scanrFile = new File(basePath + "projects_ScanR.csv");



        File questioFile = new File(basePath + "projects_Questio.csv");

        File p3File = new File(basePath + "projects_P3.csv");

        //read data from csv Files
        try {

            new CSVRecordReader(-1).loadFromCSV(
                    openaireFile, openaire);

            new CSVRecordReader(-1).loadFromCSV(
                    scanrFile, scanr);

            new CSVRecordReader(-1).loadFromCSV(
                    ariannaFile, arianna);

            new CSVRecordReader(-1).loadFromCSV(
                    questioFile, questio);

            new CSVRecordReader(-1).loadFromCSV(
                    p3File, p3);


        } catch (IOException e) {

            e.printStackTrace();
        }

        System.out.println("Number of items in Openaire: " + openaire.size());
        System.out.println("Number of items in scan r: " + scanr.size());
        System.out.println("Number of items in arianna: " + arianna.size());
        System.out.println("Number of items in questio: " + questio.size());
        System.out.println("Number of items in p3: " + p3.size());
        List<DataSet<Record, Attribute>> allDatasets = new ArrayList<>();

        allDatasets.add(openaire);
        allDatasets.add(arianna);
        allDatasets.add(questio);
        allDatasets.add(scanr);
        allDatasets.add(p3);


        List<String> datasetITNames = new ArrayList<>();
        datasetITNames.add("openaire");
        datasetITNames.add("arianna");
        datasetITNames.add("questio");
        datasetITNames.add("scanr");

        datasetITNames.add("p3");


        StatGeneratorProject statsGeneratator = new StatGeneratorProject();

        for (int i = 0; i < allDatasets.size(); i++) {
            DataSet<Record, Attribute> dataset = allDatasets.get(i);
            String name = datasetITNames.get(i);

            Collection<Record> records = dataset.get();
            System.out.println("" + name);

            Map<Attribute, Integer> stats = statsGeneratator.generateStats(records);
            statsGeneratator.printStats(records, stats);

        }


    }


    public StatGeneratorProject() {

    }


    public Map<Attribute, Integer> generateStats(Collection<Record> records) {

        Map<Attribute, Integer> attributeNonNullValue = new HashMap<>();
        List<Attribute> attrs = Project.getRelevantAttributes();
        for (Record org : records) {

            for (Attribute att : attrs) {
                String value = org.getValue(att);
                boolean isNull = isNullValue(value);

                if (!isNull) {

                    if (attributeNonNullValue.containsKey(att)) {

                        int count = attributeNonNullValue.get(att);

                        count = count + 1;
                        attributeNonNullValue.remove(att);
                        attributeNonNullValue.put(att, count);


                    } else {
                        attributeNonNullValue.put(att, 1);
                    }
                }
            }


        }
        return attributeNonNullValue;


    }

    public void printStats(Collection<Record> records, Map<Attribute, Integer> stats) {
        int nRecords = records.size();


        for (Map.Entry<Attribute, Integer> att : stats.entrySet()) {
            double coverage = (att.getValue() + 0.0d) / (nRecords + 0.0d);
            System.out.println("\t" + att.getKey().toString() + "\t" + coverage);
        }


    }

    private boolean isNullValue(String value) {
        if (value == null) {
            return true;

        } else {
            if (value.trim().equals("")) {
                return true;
            } else {
                return false;

            }
        }
    }

}
