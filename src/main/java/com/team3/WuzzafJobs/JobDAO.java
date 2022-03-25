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

import java.awt.*;
import java.io.IOException;
import java.util.*;
import java.util.List;
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
        Dataset<Row> data2 = data1.na().drop().dropDuplicates();
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
                .sql ("SELECT Company,Count(*) AS JobsCount FROM Wuzzuf_DF GROUP BY Company ORDER BY Count(*) DESC ");
        List<Row> top_Companies = demandingCompany.collectAsList();
        return DisplayHtml.displayrows(demandingCompany.columns(), top_Companies);
    }

    // Count the jobs for each company and display that in a pie chart
    public String jobsByCompanyPieChart() throws IOException {
        Dataset<Row> company = data.groupBy("Company").count().orderBy(col("count").desc()).limit(10);
        List<String> companies = company.select("Company").as(Encoders.STRING()).collectAsList();
        List<String> counts = company.select("count").as(Encoders.STRING()).collectAsList();

        PieChart chart = new PieChartBuilder().width(800).height(600).title("The Most Demanding Companies For Jobs").theme(Styler.ChartTheme.XChart).build();
        Color[] sliceColors = new Color[] { new Color(212, 157, 218),
                                            new Color(198,163, 200),
                                            new Color(200, 167, 228),
                                            new Color(187, 173, 239),
                                            new Color(160, 140, 180),
                                            new Color(175, 156, 193),
                                            new Color(97, 63, 129),
                                            new Color(78, 43, 129),
                                            new Color(69, 9, 121),
                                            new Color(70, 15, 121) };
        chart.getStyler().setSeriesColors(sliceColors);

        for (int i = 0; i < 10; i++) {
            chart.addSeries(companies.get(i), Integer.parseInt(counts.get(i)));
        }
        String path = "src/main/resources/files/jobsByCompanyPieChart.jpg";
        try {
            BitmapEncoder.saveJPGWithQuality(chart, path, 0.98f);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return DisplayHtml.viewchart(path);
    }

    //The most popular job titles
    public String JobsByTitles(){
        Dataset<Row> title = data.groupBy("Title").count().orderBy(col("count").desc());
        List<Row> top_titles = title.collectAsList();
        return DisplayHtml.displayrows(title.columns(), top_titles);
    }

    // The most popular job titles Bar chart
    public String PopularJobTitlesBarChart() throws IOException {
        Dataset<Row> title = data.groupBy("Title").count().orderBy(col("count").desc()).limit(10);
        List<String> titles = title .select("Title").as(Encoders.STRING()).collectAsList();
        List<Long> counts = title .select("count").as(Encoders.LONG()).collectAsList();

        CategoryChart chart = new CategoryChartBuilder().width(800).height(600).title("The Most Popular Job Titles").xAxisTitle("Title").yAxisTitle("Jobs").theme(Styler.ChartTheme.GGPlot2).build();
        chart.getStyler().setHasAnnotations(true);
        chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideNE);
        chart.getStyler().setStacked(true);
        chart.getStyler().setXAxisLabelRotation(90);
        chart.getStyler().setSeriesColors(new Color[]{new Color(97, 63, 129), Color.cyan, Color.LIGHT_GRAY, new Color(224,43,54), new Color(24,43,124)});
        chart.addSeries("Popular Job Titles", titles, counts);

        String path = "src/main/resources/files/PopularJobTitlesBarChart.jpg";
        try {
            BitmapEncoder.saveJPGWithQuality(chart, path, 0.98f);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return DisplayHtml.viewchart(path);
    }

    // The most popular areas
    public String JobsByAreas(){
        Dataset<Row> area = data.groupBy("Location").count().orderBy(col("count").desc());;
        List<Row> top_titles = area.collectAsList();
        return DisplayHtml.displayrows(area.columns(), top_titles);
    }

    // The most popular areas Bar chart
    public String PopularAreasBarChart() throws IOException {
        Dataset<Row> area = data.groupBy("Location").count().orderBy(col("count").desc()).limit(10);
        List<String> location = area.select("Location").as(Encoders.STRING()).collectAsList();
        List<Long> counts = area.select("count").as(Encoders.LONG()).collectAsList();


        CategoryChart chart = new CategoryChartBuilder().width(800).height(600).title("The Most Popular Areas").xAxisTitle("Location").yAxisTitle("Jobs").theme(Styler.ChartTheme.GGPlot2).build();
        chart.getStyler().setHasAnnotations(true);
        chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideNE);
        chart.getStyler().setStacked(true);
        chart.getStyler().setXAxisLabelRotation(90);
        chart.getStyler().setSeriesColors(new Color[]{new Color(97, 63, 129), Color.cyan, Color.LIGHT_GRAY, new Color(224,43,54), new Color(24,43,124)});
        chart.addSeries("Popular Areas",location, counts);

        String path = "src/main/resources/files/PopularAreasBarChart.jpg";
        try {
            BitmapEncoder.saveJPGWithQuality(chart, path, 0.98f);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return DisplayHtml.viewchart(path);

    }

    //Find out the most important skills required
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
    public String fatorizeYearsExp(){
        Dataset<Row> newDs = new StringIndexer().setInputCol("YearsExp")
                .setOutputCol("YearsExpEncoded")
                .fit(data)
                .transform(data);
        String columns[] = {"YearsExp", "YearsExpEncoded"};
        List<Row> yeasExpEncoded = newDs.select("YearsExp", "YearsExpEncoded").distinct().orderBy(col("YearsExpEncoded").asc()).collectAsList();
        return DisplayHtml.displayrows(columns, yeasExpEncoded);
    }
    // Display the dataset after factorization
    public String datasetAfterFatorization(){
        Dataset<Row> newDs = getDatasetCleaning();
        Dataset<Row> newDs1 = new StringIndexer().setInputCol("YearsExp")
                .setOutputCol("YearsExpIndex")
                .fit(newDs)
                .transform(newDs);
        //Dataset<Row> dataset = newDs1.select("YearsExpIndex");
        //data = data.withColumn("YearsExpEncoded", dataset.col("YearsExpIndex"));
        String[] columns = {"Title", "Company", "Location", "Type", "Level", "YearsExp", "YearsExpEncoded", "Country", "Skills"};
        List<Row> yeasExpEncoded = newDs1.select("Title", "Company", "Location", "Type", "Level", "YearsExp", "YearsExpIndex", "Country", "Skills").collectAsList();
        return DisplayHtml.displayrows(columns, yeasExpEncoded);
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