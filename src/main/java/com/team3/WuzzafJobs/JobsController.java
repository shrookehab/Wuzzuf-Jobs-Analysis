package com.team3.WuzzafJobs;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import java.io.IOException;

@RestController
public class JobsController {

    JobDAO service = new JobDAO();
    @GetMapping("/dateset_before_cleaning")
    public String dateset_before_cleaning(){
        return service.ShowDataBeforeCleaning();
    }

    @GetMapping("/dateset_after_cleaning")
    public String dateset_after_cleaning(){
        return service.ShowDataAfterCleaning();
    }

    @GetMapping("/dataset_structure")
    public String dataset_structure() throws IOException {
        return service.structure();
    }

    @GetMapping("/dataset_summary")
    public String dataset_summary(){
        return service.summary();
    }


    @GetMapping("/top_companies")
    public String top_companies(){
        return service.jobsByCompany();
    }

    @GetMapping("/top_titles")
    public String top_titles(){
        return service.JobsByTitles();
    }

    @GetMapping("/top_areas")
    public String top_areas(){
        return service.JobsByAreas();
    }

    @GetMapping("/pie_chart")
    public String pie_chart() throws IOException {
        return service.pieChartForCompany();
    }

    @GetMapping("/title_bar_chart")
    public String title_bar_chart() throws IOException {
        return service.TitlesBarChart();
    }

    @GetMapping("/location_bar_chart")
    public String location_bar_chart() throws IOException {
        return service.areasBarChart();
    }

    @GetMapping("/top_skills")
    public String top_skills() {
        return service.mostImportantSkills();
    }

    @GetMapping("/fact_Years_Exp")
    public String fact_Years_Exp()  {
        return service.fatorizeYearsExp();
    }

    @GetMapping("/dataset_after_factorization")
    public String dataset_after_factorization()  {
        return service.datasetAfterFatorization();
    }

    @GetMapping("/KMeans_Clustering")
    public  String  KMeans_Clustering()  {
        return service.kMeansClustering();
    }
}
