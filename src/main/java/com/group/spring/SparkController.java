package com.group.spring;

import org.apache.spark.sql.*;
import org.knowm.xchart.*;
import com.group.spring.model.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import scala.Tuple2;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

@RequestMapping("spark-context")
@Controller
public class SparkController {
    @Autowired
    private SparkSession sparkSession;

    private Dataset<Job> jobDataset;
    private Dataset<Row> jobCountPerCompany;
    private Dataset<Row> popularTitles;
    private Dataset<Row> popularLocations;
    private Dataset<Row> popularSkills;

    // 1)
    @RequestMapping("read-csv")
    @ResponseBody
    public List<Job> getRowCount() throws IOException {
        //1)
        //=============================================================================================================
        Dataset<Row> dataset = sparkSession.read().option("header", "true")
                .csv("C:\\Users\\omarh\\IdeaProjects\\spark-spring-boot-master\\src\\main\\resources\\Wuzzuf_Jobs.csv");


        jobDataset = dataset.as(Encoders.bean(Job.class));
        List<Job> jobs = jobDataset.collectAsList();

        //2)
        //=============================================================================================================
        // Drop rows with null values and duplicates
        Dataset<Row> cleanedJobsData = jobDataset.na().drop().dropDuplicates();
        jobDataset = cleanedJobsData.as(Encoders.bean(Job.class));


        //3)
        //=============================================================================================================
        // Count of Jobs per company
        jobDataset.createOrReplaceTempView ("JOBS_DATA");
        jobCountPerCompany = sparkSession.sql("SELECT Company, count(Title) as freq "+
                                                     "FROM JOBS_DATA GROUP BY Company "+
                                                     "ORDER BY freq DESC ");

        //4)
        //=============================================================================================================
        // Prepare data for pie chart
        List<String> companyNames = jobCountPerCompany.select("Company").limit(10).as(Encoders.STRING()).collectAsList();
        List<Long> jobCount = jobCountPerCompany.select("freq").limit(10).as(Encoders.LONG()).collectAsList();

        // Make pie chart of above data
        PieChart pieChart1 = new PieChartBuilder().width(1280).height(800).title("Jobs Per Company").build();
        for (int i=0; i<companyNames.size(); i++){
            pieChart1.addSeries(companyNames.get(i), jobCount.get(i));
        }
        BitmapEncoder.saveBitmap(pieChart1, "src/main/resources/jobCountPerCompany", BitmapEncoder.BitmapFormat.PNG);


        //5)
        //=============================================================================================================
        // Frequency of job titles
        jobDataset.createOrReplaceTempView ("JOBS_DATA");
        popularTitles = sparkSession.sql("SELECT Title, count(Title) as freq "+
                                                "FROM JOBS_DATA GROUP BY Title "+
                                                "ORDER BY freq DESC ");

        //6)
        //=============================================================================================================
        // Prepare data for bar chart
        List<String> jobTitles = popularTitles.select("Title").limit(10).as(Encoders.STRING()).collectAsList();
        List<Long> jobCount2 = popularTitles.select("freq").limit(10).as(Encoders.LONG()).collectAsList();

        // Make bar chart of above data
        CategoryChart barChart = new CategoryChartBuilder().width(1280).height(800).title("Popular Job Titles").build();

        barChart.addSeries("Popular Job Titles", jobTitles, jobCount2);

        BitmapEncoder.saveBitmap(barChart, "src/main/resources/popularTitles", BitmapEncoder.BitmapFormat.PNG);

        //7)
        //=============================================================================================================
        // popular areas
        jobDataset.createOrReplaceTempView ("JOBS_DATA");
        popularLocations = sparkSession.sql("SELECT Location, count(Location) as freq "+
                                                  "FROM JOBS_DATA GROUP BY Location "+
                                                  "ORDER BY freq DESC ");

        //8)
        //=============================================================================================================

        // Prepare data for bar chart
        List<String> locations = popularLocations.select("Location").limit(10).as(Encoders.STRING()).collectAsList();
        List<Long> locCount = popularLocations.select("freq").limit(10).as(Encoders.LONG()).collectAsList();

        // Make bar chart of above data
        CategoryChart barChart2 = new CategoryChartBuilder().width(1280).height(800).title("Popular Locations").build();

        barChart2.addSeries("Popular Locations Titles", locations, locCount);

        BitmapEncoder.saveBitmap(barChart2, "src/main/resources/popularLocations", BitmapEncoder.BitmapFormat.PNG);

        //9)
        //=============================================================================================================
        // Most required skills
        popularSkills = jobDataset.select("Skills")
                .flatMap(row -> Arrays.asList(row.getString(0).split(",")).iterator(), Encoders.STRING())
                .filter(s -> !s.isEmpty())
                .map(word -> new Tuple2<>(word.toLowerCase(), 1L), Encoders.tuple(Encoders.STRING(), Encoders.LONG()))
                .toDF("word", "count")
                .groupBy("word")
                .sum("count").orderBy(new Column("sum(count)").desc())
                .withColumnRenamed("sum(count)", "cnt");

        return  jobs;
    }

    // 2)
    @RequestMapping("statistics")
    public ResponseEntity<String> getStatistics() {

        String html = String.format("<h1>%s</h1>", "Wuzzuf Jobs Data Statistics") +
                String.format("<h3>%s</h3>", "Data Summary: ") +
                String.format("<h4>Total records %d</h4>", jobDataset.count()) +
                String.format("<h5>Schema <br/> %s</h5> <br/> Sample data - <br/>", jobDataset.summary()
                        .showString(1, 20, true)) +
                jobDataset.showString(20, 20, true);
        return ResponseEntity.ok(html);
    }

    // 3)
    @RequestMapping("clean-data")
    public ResponseEntity<String> getCleanData() {

        String html = String.format("<h1>%s</h1>", "Wuzzuf Jobs Cleaned Data") +
                String.format("<h3>%s</h3>", "Clean Data: ") +
                String.format("<h4>Total records %d</h4>", jobDataset.count()) +
                jobDataset.showString(20, 1, true);

        return ResponseEntity.ok(html);
    }

    // 4)
    @RequestMapping("jobs-per-company")
    public ResponseEntity<String> jobsPerCompany() {

        String html = String.format("<h1>%s</h1>", "Wuzzuf Jobs ") +
                String.format("<h3>%s</h3>", "Jobs per Company") +
                String.format("<h4>Total records %d</h4>", jobCountPerCompany.count()) +
                String.format("<h5>Schema <br/> %s</h5> <br/> Sample data - <br/>", jobCountPerCompany.schema().treeString()) +
                jobCountPerCompany.showString(20, 20, true);
        return ResponseEntity.ok(html);
    }

    @RequestMapping (value = "jobs-per-company-chart-image", produces = MediaType.IMAGE_PNG_VALUE)
    public ResponseEntity<Resource> jobsPerCompanyChartImage() throws IOException {
        final ByteArrayResource inputStream = new ByteArrayResource(Files.readAllBytes(Paths.get(
                    "src/main/resources/jobCountPerCompany.png")));
        return ResponseEntity
                    .status(HttpStatus.OK)
                    .contentLength(inputStream.contentLength())
                    .body(inputStream);
    }

    // 6)
    @RequestMapping("popular-titles")
    public ResponseEntity<String> popularTitles() {

        String html = String.format("<h1>%s</h1>", "Wuzzuf Jobs Popular Titles:") +
                String.format("<h3>%s</h3>", "Popular Titles") +
                String.format("<h5>Schema <br/> %s</h5> <br/> Sample data - <br/>", popularTitles.schema().treeString()) +
                popularTitles.showString(20, 20, true);
        return ResponseEntity.ok(html);
    }


    @RequestMapping (value = "popular-titles-chart-image", produces = MediaType.IMAGE_PNG_VALUE)
    public ResponseEntity<Resource> popularTitlesChartImage() throws IOException {
        final ByteArrayResource inputStream = new ByteArrayResource(Files.readAllBytes(Paths.get(
                "src/main/resources/popularTitles.png")));
        return ResponseEntity
                .status(HttpStatus.OK)
                .contentLength(inputStream.contentLength())
                .body(inputStream);
    }

    // 8)
    @RequestMapping("popular-locations")
    public ResponseEntity<String> popularLocations() {

        String html = String.format("<h1>%s</h1>", "Wuzzuf Jobs Popular Locations:") +
                String.format("<h3>%s</h3>", " Popular Locations") +
                String.format("<h5>Schema <br/> %s</h5> <br/> Sample data - <br/>", popularLocations.schema().treeString()) +
                popularLocations.showString(20, 20, true);
        return ResponseEntity.ok(html);
    }


    @RequestMapping (value = "popular-locations-chart-image", produces = MediaType.IMAGE_PNG_VALUE)
    public ResponseEntity<Resource> popularLocationsChartImage() throws IOException {
        final ByteArrayResource inputStream = new ByteArrayResource(Files.readAllBytes(Paths.get(
                "src/main/resources/popularLocations.png")));
        return ResponseEntity
                .status(HttpStatus.OK)
                .contentLength(inputStream.contentLength())
                .body(inputStream);
    }

    // 10)
    @RequestMapping("skills")
    public ResponseEntity<String> getSkills() {
        String html = String.format("<h1>%s</h1>", "Most Popular Skills:") +
                String.format("<h3>%s</h3>", "Popular Skills") +
                String.format("<h5>Schema <br/> %s</h5> <br/> Sample data - <br/>", popularSkills.summary()
                        .showString(1, 20, true)) +
                popularSkills.showString(20, 20, true);
        return ResponseEntity.ok(html);
    }

}
