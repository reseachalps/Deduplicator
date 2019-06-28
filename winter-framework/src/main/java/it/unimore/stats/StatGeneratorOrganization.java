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

public class StatGeneratorOrganization {


    public static void main(String arg[]) {

        // DATASETS
        DataSet<Record, Attribute> openaire = new HashedDataSet<>();
        DataSet<Record, Attribute> cercauniversita = new HashedDataSet<>();
        DataSet<Record, Attribute> arianna = new HashedDataSet<>();
        DataSet<Record, Attribute> questio = new HashedDataSet<>();
        DataSet<Record, Attribute> bvd = new HashedDataSet<>();
        DataSet<Record, Attribute> cnr = new HashedDataSet<>();
        DataSet<Record, Attribute> scanr = new HashedDataSet<>();
        DataSet<Record, Attribute> orcid = new HashedDataSet<>();
        DataSet<Record, Attribute> registroImprese = new HashedDataSet<>();
        DataSet<Record, Attribute> patiris = new HashedDataSet<>();
        DataSet<Record, Attribute> all = new HashedDataSet<>();

        // /Users/paolosottovia/Downloads/extracted_data_17_03_2018/

        String basePath = "/Users/paolosottovia/Downloads/new_exported_csv_data/";

//        String basePath = arg[0];
//        String mode = arg[1];

        System.out.println("BASEPATH: " + basePath);
//        System.out.println("MODE: " + mode);


        File cercaUniversitaFile = new File(basePath + "organizations_CercaUniversita.csv");

        File openaireFile = new File(basePath + "organizations_OpenAire.csv");

        File questioFile = new File(basePath + "organizations_Questio.csv");

        File ariannaFile = new File(basePath + "organizations_Arianna - Anagrafe Nazionale delle Ricerche.csv");

        File dvbFile = new File(basePath + "organizations_Bureau van Dijk.csv");

        File cnrFile = new File(basePath + "organizations_Consiglio Nazionale delle Ricerche (CNR).csv");

        File scanrFile = new File(basePath + "organizations_ScanR.csv");

        File orcidFile = new File(basePath + "organizations_ORCID.csv");

        File registroImpreseFile = new File(basePath + "organizations_Startup - Registro delle imprese.csv");

        File patirisFile = new File(basePath + "organizations_Patiris.csv");



        //read data from csv Files
        try {
            new CSVRecordReader(-1).loadFromCSV(
                    cercaUniversitaFile,
                    cercauniversita);
            new CSVRecordReader(-1).loadFromCSV(
                    openaireFile, openaire);

            new CSVRecordReader(-1).loadFromCSV(
                    questioFile, questio);

            new CSVRecordReader(-1).loadFromCSV(
                    ariannaFile, arianna);

            new CSVRecordReader(-1).loadFromCSV(
                    dvbFile, bvd);

            new CSVRecordReader(-1).loadFromCSV(
                    cnrFile, cnr);

            new CSVRecordReader(-1).loadFromCSV(
                    scanrFile, scanr);

            new CSVRecordReader(-1).loadFromCSV(
                    orcidFile, orcid);

            new CSVRecordReader(-1).loadFromCSV(
                    registroImpreseFile, registroImprese);

            new CSVRecordReader(-1).loadFromCSV(
                    patirisFile, patiris);

        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("\nNumber of items in Openaire: " + openaire.size());
        System.out.println("Number of items in cerca universita: " + cercauniversita.size());
        System.out.println("Number of items in questio: " + questio.size());
        System.out.println("Number of items in arianna: " + arianna.size());
        System.out.println("Number of items in bvd: " + bvd.size());
        System.out.println("Number of items in cnr: " + cnr.size());
        System.out.println("Number of items in scanr: " + scanr.size());
        System.out.println("Number of items in orcid: " + orcid.size());
        System.out.println("Number of items in registro imprese: " + registroImprese.size());
        System.out.println("Number of items in patiris: " + patiris.size());

        List<DataSet<Record, Attribute>> datasetsIT = new ArrayList<>();
//        List<DataSet<Record, Attribute>> datasetsFR = new ArrayList<>();
        datasetsIT.add(openaire);
        datasetsIT.add(cercauniversita);
        datasetsIT.add(questio);
        datasetsIT.add(arianna);
        datasetsIT.add(bvd);
        datasetsIT.add(cnr);
        datasetsIT.add(orcid);
        datasetsIT.add(registroImprese);
        datasetsIT.add(patiris);

        List<String> datasetITNames = new ArrayList<>();
        datasetITNames.add("openaire");
        datasetITNames.add("cercauniversita");
        datasetITNames.add("questio");
        datasetITNames.add("arianna");
        datasetITNames.add("bvd");
        datasetITNames.add("cnr");
        datasetITNames.add("orcid");
        datasetITNames.add("registo imprese");
        datasetITNames.add("patiris");

        StatGeneratorOrganization statsGeneratator = new StatGeneratorOrganization();

        for (int i = 0; i < datasetsIT.size(); i++) {
            DataSet<Record, Attribute> dataset = datasetsIT.get(i);
            String name = datasetITNames.get(i);

            Collection<Record> records = dataset.get();
            System.out.println("" + name);

            Map<Attribute, Integer> stats = statsGeneratator.generateStats(records);
            statsGeneratator.printStats(records, stats);

        }


    }


    public StatGeneratorOrganization() {

    }


    public Map<Attribute, Integer> generateStats(Collection<Record> records) {

        Map<Attribute, Integer> attributeNonNullValue = new HashMap<>();
        List<Attribute> attrs = Organization.getRelevantAttributes();
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
