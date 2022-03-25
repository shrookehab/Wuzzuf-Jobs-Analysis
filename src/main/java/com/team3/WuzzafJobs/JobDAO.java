package com.team3.WuzzafJobs;

import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.knowm.xchart.*;
import org.knowm.xchart.style.Styler;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.apache.spark.ml.linalg.Vector;
import tech.tablesaw.api.Table;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;


import static org.apache.spark.sql.functions.*;

public class JobDAO{

    // Create Spark Session to create connection to Spark
    public Dataset<Row> getDataset() {
        final SparkSession session  = SparkSession.builder().appName("Wuzzuf Jobs Project").master("local[4]").getOrCreate();
        DataFrameReader Dataframe = session.read();
        Dataset<Row> data = Dataframe.option("header", "true").csv("src/main/resources/files/Wuzzuf_Jobs.csv");
        return data;
    }

    Dataset<Row> data1 = getDataset();

    public String ShowDataBeforeCleaning(){
        List<Row> first_20_records = data1.limit(50).collectAsList();
        return DisplayHtml.displayrows(data1.columns(), first_20_records);
    }



    public Dataset<Row> getDatasetCleaning() {
        Dataset<Row> data2 = data1.na().drop().distinct();
        return data2;
    }

    Dataset<Row> data = getDatasetCleaning();

    public String ShowDataAfterCleaning(){
        List<Row> first_20_records = data.limit(50).collectAsList();
        return DisplayHtml.displayrows(data.columns(), first_20_records);
    }

    // Print Schema to see column names, types and other metadata
    public String structure() throws IOException {
        Table wuzzufData= Table.read().csv("src/main/resources/files/Wuzzuf_Jobs.csv");
        String[] splitedData = wuzzufData.structure().toString().split("\\n", 3);
        String[] columns = {"Index", "Column Name", "Column Type"};
        return DisplayHtml.displayStrings(columns, splitedData);
    }

    // Print summary
    public String summary() {
        Dataset<Row> d = data.summary();
        List<Row> summary = d.collectAsList();
        return DisplayHtml.displayrows(d.columns(), summary);
    }

    // Count the jobs for each company and display that in order
    public String jobsByCompany(){
        //Dataset<Row> company = data.groupBy("Company").count().orderBy(col("count").desc()).limit(20);
        data.createOrReplaceTempView ("Wuzzuf_DF");
        final SparkSession sparkSession = SparkSession.builder ().appName ("Wuzzuf Jobs Project").master ("local[4]")
                .getOrCreate ();
        final Dataset<Row> demandingCompany = sparkSession
                .sql ("SELECT Company,Count(*) AS Available_Jobs FROM Wuzzuf_DF GROUP BY Company ORDER BY Count(*) DESC ");
        List<Row> top_Companies = demandingCompany.collectAsList();
        return DisplayHtml.displayrows(demandingCompany.columns(), top_Companies);
    }

    //Show  previous data in a pie chart
    public String pieChartForCompany() throws IOException {
        Dataset<Row> company = data.groupBy("Company").count().orderBy(col("count").desc()).limit(10);
        List<String> companies = company.select("Company").as(Encoders.STRING()).collectAsList();
        List<String> counts = company.select("count").as(Encoders.STRING()).collectAsList();

        PieChart chart = new PieChartBuilder().width(800).height(600).title("Most Demanding Companies For Jobs").build();
        for (int i = 0; i < 10; i++) {
            chart.addSeries(companies.get(i), Integer.parseInt(counts.get(i)));
        }
        String path = "src/main/resources/files/pieChart.png";
        return DisplayHtml.viewchart(path);
    }

    //Most popular job titles
    public String JobsByTitles(){
        Dataset<Row> title = data.groupBy("Title").count().orderBy(col("count").desc());
        List<Row> top_titles = title.collectAsList();
        return DisplayHtml.displayrows(title.columns(), top_titles);
    }

    // Bar chart of the previous data
    public String TitlesBarChart() throws IOException {
        Dataset<Row> title = data.groupBy("Title").count().orderBy(col("count").desc()).limit(10);
        List<String> titles = title .select("Title").as(Encoders.STRING()).collectAsList();
        List<Long> counts = title .select("count").as(Encoders.LONG()).collectAsList();

        CategoryChart bar = new CategoryChartBuilder().width(800).height(600).title("Most popular job titles").xAxisTitle("Title").yAxisTitle("Jobs").build();
        bar.getStyler().setHasAnnotations(true);
        bar.getStyler().setLegendPosition(Styler.LegendPosition.InsideNE);
        bar.getStyler().setStacked(true);
        bar.getStyler().setXAxisLabelRotation(45);
        bar.addSeries("Jobs per title", titles, counts);

        String path = "src/main/resources/files/barChart1.png";
        return DisplayHtml.viewchart(path);
    }

    // Most popular areas
    public String JobsByAreas(){
        Dataset<Row> area = data.groupBy("Location").count().orderBy(col("count").desc());;
        List<Row> top_titles = area.collectAsList();
        return DisplayHtml.displayrows(area.columns(), top_titles);
    }

    // Bar chart of the previous data
    public String areasBarChart() throws IOException {
        Dataset<Row> area = data.groupBy("Location").count().orderBy(col("count").desc()).limit(10);
        List<String> location = area.select("Location").as(Encoders.STRING()).collectAsList();
        List<Long> counts = area.select("count").as(Encoders.LONG()).collectAsList();

        CategoryChart bar2 = new CategoryChartBuilder().width(800).height(600).title("Most popular job locations").xAxisTitle("Location").yAxisTitle("Jobs").build();
        bar2.getStyler().setHasAnnotations(true);
        bar2.getStyler().setLegendPosition(Styler.LegendPosition.InsideNE);
        bar2.getStyler().setStacked(true);
        bar2.getStyler().setXAxisLabelRotation(45);
        bar2.addSeries("Jobs per location", location, counts);

        String path = "src/main/resources/files/barChart2.png";
        return DisplayHtml.viewchart(path);

    }

    public String mostImportantSkills(){
        List<String> allSkills = data.select("Skills").as(Encoders.STRING()).collectAsList();
        List<String> skills = new ArrayList<>();
        for (String ls : allSkills) {
            String[] x = ls.split(",");
            for (String s : x) {
                skills.add(s);
            }
        }

        Map<String, Long> skill_counts =
                skills.stream().collect(Collectors.groupingBy(e -> e, Collectors.counting()));

        Map<String, Long> mostImportantSkills =
                skill_counts.entrySet().stream()
                        .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                        .collect(Collectors.toMap(
                                Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

        String[] columns = {"Most Important Skills", "Count"};
        return DisplayHtml.displayMap(columns, mostImportantSkills);
    }

    // Factorize Years Exp feature in the original data
    public String factYearsExp(){
        StringIndexer idx = new StringIndexer();
        idx.setInputCol("YearsExp").setOutputCol("YearsExp indexed");
        Dataset<Row> new_data = idx.fit(data).transform(data);
        String columns[] = {"YearsExp", "YearsExp indexed"};
        List<Row> yeasExpIndexed = new_data.select("YearsExp", "YearsExp indexed").limit(30).collectAsList();
        return DisplayHtml.displayrows(columns, yeasExpIndexed);
    }
    //Apply K-means for job title and companies
    public String kMeansClustering(){
        //final SparkSession session  = SparkSession.builder().appName("Wuzzuf Jobs Project").master("local[4]").getOrCreate();
        //Dataset<Row> dataset = session.read().format("libsvm").load("src/main/resources/files/Wuzzuf_Jobs.csv");
        Dataset<Row> dataset = data.select("Title", "Company");
        KMeans kmeans = new KMeans().setK(2).setSeed(1L);
        KMeansModel model = kmeans.fit(dataset);
        // Make predictions
        Dataset<Row> predictions = model.transform(dataset);
        // Evaluate clustering by computing Silhouette score
        ClusteringEvaluator evaluator = new ClusteringEvaluator();
        double silhouette = evaluator.evaluate(predictions);
        System.out.println("Silhouette Score with squared euclidean distance = " + silhouette);
//        double WSSSE = model.computeCost(data);
//        System.out.println("Within Set Sum of Squared Errors = " + WSSSE);
        // Shows the result.
        Vector[] centers = model.clusterCenters();
        String columns[] = {"Cluster Centers"};
        for (Vector center: centers) {
            System.out.println(center);
        }
        //return DisplayHtml.displayrows(columns, centers.collectAsList());
        return "";
    }

}