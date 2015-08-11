package com.awesome;

import java.util.List;
import java.util.ArrayList;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import org.kie.api.KieBase;
import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;

public class App {
  public static void main(String[] args) {
    List<Applicant> inputData = new ArrayList<Applicant>();
    inputData.add(new Applicant(1, "John", "Doe", 10000, 568));

    SparkConf conf = new SparkConf().setAppName("Simple Application");
    JavaSparkContext sc = new JavaSparkContext(conf);

    KieBase rules = loadRules();
    Broadcast<KieBase> broadcastRules = sc.broadcast(rules);

    JavaRDD<Applicant> logData = sc.parallelize(inputData);

    long numApproved = logData.filter( (Applicant a) -> a.isApproved() ).count();

    System.out.println("Number of applicants approved: " + numApproved);
  }

  public static KieBase loadRules() {
    KieServices kieServices = KieServices.Factory.get();
    KieContainer kieContainer = kieServices.getKieClasspathContainer();

    return kieContainer.getKieBase();
  }
}
