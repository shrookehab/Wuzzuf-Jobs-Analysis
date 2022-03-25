package com.team3.WuzzafJobs;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import java.io.IOException;

@RestController
public class JobsController {

    JobDAO service = new JobDAO();
    @GetMapping("/dateset_before_cleaning")
    public  String  dateset_before_cleaning(){
        return service.ShowDataBeforeCleaning();
    }

    @GetMapping("/dateset_after_cleaning")
    public  String  dateset_after_cleaning(){
        return service.ShowDataAfterCleaning();
    }

    @GetMapping("/show_structure")
    public  String  show_structure() throws IOException {
        return service.structure();
    }

    @GetMapping("/show_summary")
    public  String  show_summary(){
        return service.summary();
    }


    @GetMapping("/show_top_companies")
    public  String  show_top_companies(){
        return service.jobsByCompany();
    }

    @GetMapping("/show_top_titles")
    public  String  show_top_titles(){
        return service.JobsByTitles();
    }

    @GetMapping("/show_top_areas")
    public  String  show_top_countries(){
        return service.JobsByAreas();
    }

    @GetMapping("/show_pie_chart")
    public  String  show_pie_chart() throws IOException {
        return service.pieChartForCompany();
    }

    @GetMapping("/title_bar_chart")
    public  String  title_bar_chart() throws IOException {
        return service.TitlesBarChart();
    }

    @GetMapping("/location_bar_chart")
    public  String  location_bar_chart() throws IOException {
        return service.areasBarChart();
    }

    @GetMapping("/show_top_skills")
    public String show_top_skills() {
        return service.mostImportantSkills();
    }

    @GetMapping("/show_YearsExp")
    public  String  show_YearsExp()  {
        return service.factYearsExp();
    }

    @GetMapping("/KMeans_Clustering")
    public  String  KMeans_Clustering()  {
        return service.kMeansClustering();
    }
}
