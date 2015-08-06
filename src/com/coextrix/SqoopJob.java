/**
 * 
 */
package com.coextrix;

import java.util.List;

import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.model.MConnectionForms;
import org.apache.sqoop.model.MForm;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MJobForms;
import org.apache.sqoop.validation.Status;

/**
 * @author milan
 *
 */
public class SqoopJob {
	String url = "http://localhost:12000/sqoop/";
	SqoopClient client = new SqoopClient(url);
	
	//Dummy connection object
	MConnection newCon1 = client.newConnection(1);

	//Get connection and framework forms. Set name for connection
	MConnectionForms conForms1 = newCon1.getConnectorPart();
	MConnectionForms frameworkForms = newCon1.getFrameworkPart();
	//newCon.setName();
    
	//Set connection forms values
	
	conForms1.getStringInput("connection.connectionString").setValue("jdbc:mysql://localhost/my");
	conForms.getStringInput("connection.jdbcDriver").setValue("com.mysql.jdbc.Driver");
	conForms.getStringInput("connection.username").setValue("root");
	conForms.getStringInput("connection.password").setValue("root");

	frameworkForms.getIntegerInput("security.maxConnections").setValue(0);

	Status status  = client.createConnection(newCon1);
	if(status.canProceed()) {
	 System.out.println("Created. New Connection ID : " +newCon1.getPersistenceId());
	} else {
	 System.out.println("Check for status and forms error ");
	}
	
	
	
	private static void printMessage(List<MForm> formList) {
	  for(MForm form : formList) {
	    List<MInput<?>> inputlist = form.getInputs();
	    if (form.getValidationMessage() != null) {
	      System.out.println("Form message: " + form.getValidationMessage());
	    }
	    for (MInput minput : inputlist) {
	      if (minput.getValidationStatus() == Status.ACCEPTABLE) {
	        System.out.println("Warning:" + minput.getValidationMessage());
	      } else if (minput.getValidationStatus() == Status.UNACCEPTABLE) {
	        System.out.println("Error:" + minput.getValidationMessage());
	      }
	    }
	  }
	}
	
	
	//Creating dummy job object
	MJob newjob = client.newJob(1, org.apache.sqoop.model.MJob.Type.IMPORT);
	MJobForms connectorForm = newjob.getConnectorPart();
	MJobForms frameworkForm = newjob.getFrameworkPart();

	newjob.setName("ImportJob");
	//Database configuration
	connectorForm.getStringInput("table.schemaName").setValue("");
	//Input either table name or sql
	connectorForm.getStringInput("table.tableName").setValue("table");
	//connectorForm.getStringInput("table.sql").setValue("select id,name from table where ${CONDITIONS}");
	connectorForm.getStringInput("table.columns").setValue("id,name");
	connectorForm.getStringInput("table.partitionColumn").setValue("id");
	//Set boundary value only if required
	//connectorForm.getStringInput("table.boundaryQuery").setValue("");

	//Output configurations
	frameworkForm.getEnumInput("output.storageType").setValue("HDFS");
	frameworkForm.getEnumInput("output.outputFormat").setValue("TEXT_FILE");//Other option: SEQUENCE_FILE
	frameworkForm.getStringInput("output.outputDirectory").setValue("/output");

	//Job resources
	frameworkForm.getIntegerInput("throttling.extractors").setValue(1);
	frameworkForm.getIntegerInput("throttling.loaders").setValue(1);

	Status status1 = client.createJob(newjob);
	if(status.canProceed()) {
	 System.out.println("New Job ID: "+ newjob.getPersistenceId());
	} else {
	 System.out.println("Check for status and forms error ");
	}

	//Print errors or warnings
	printMessage(newjob.getConnectorPart().getForms());
	printMessage(newjob.getFrameworkPart().getForms());

	
	//Job submission start
	MSubmission submission = client.startSubmission(1);
	System.out.println("Status : " + submission.getStatus());
	if(submission.getStatus().isRunning() && submission.getProgress() != -1) {
	  System.out.println("Progress : " + String.format("%.2f %%", submission.getProgress() * 100));
	}
	System.out.println("Hadoop job id :" + submission.getExternalId());
	System.out.println("Job link : " + submission.getExternalLink());
	Counters counters = submission.getCounters();
	if(counters != null) {
	  System.out.println("Counters:");
	  for(CounterGroup group : counters) {
	    System.out.print("\t");
	    System.out.println(group.getName());
	    for(Counter counter : group) {
	      System.out.print("\t\t");
	      System.out.print(counter.getName());
	      System.out.print(": ");
	      System.out.println(counter.getValue());
	    }
	  }
	}
	if(submission.getExceptionInfo() != null) {
	  System.out.println("Exception info : " +submission.getExceptionInfo());
	}


	//Check job status
	MSubmission submission = client.getSubmissionStatus(1);
	if(submission.getStatus().isRunning() && submission.getProgress() != -1) {
	  System.out.println("Progress : " + String.format("%.2f %%", submission.getProgress() * 100));
	}

	//Stop a running job
	submission.stopSubmission(jid);
	
	
}
