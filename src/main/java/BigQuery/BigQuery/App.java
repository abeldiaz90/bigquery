package BigQuery.BigQuery;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryResponse;
import com.google.cloud.bigquery.TableResult;

import java.io.File;
import java.io.FileInputStream;
import java.util.UUID;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) {
		try {
			
	       //se accede con el json de manera local
//			GoogleCredentials credentials;
//			  File credentialsPath = new File("client_secret.json");  // TODO: update to your key path.
//			  FileInputStream serviceAccountStream = new FileInputStream(credentialsPath);
//			  credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
//			  
//			
//			   BigQuery bigquery = BigQueryOptions.newBuilder()
//			         .setCredentials(credentials)			         
//			         .build().getService();
			   
			//se accede con la variable de entorno
			BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
			QueryJobConfiguration queryConfig = QueryJobConfiguration
					.newBuilder("SELECT * from `third-node-122719.Banca.Personas` LIMIT 1000")
					// Use standard SQL syntax for queries.
					// See: https://cloud.google.com/bigquery/sql-reference/
					.setUseLegacySql(false).build();

			// Create a job ID so that we can safely retry.
			JobId jobId = JobId.of(UUID.randomUUID().toString());
			Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

			// Wait for the query to complete.
			queryJob = queryJob.waitFor();

			// Check for errors
			if (queryJob == null) {
				throw new RuntimeException("Job no longer exists");
			} else if (queryJob.getStatus().getError() != null) {
				// You can also look at queryJob.getStatus().getExecutionErrors() for all
				// errors, not just the latest one.
				throw new RuntimeException(queryJob.getStatus().getError().toString());
			}

			// Get the results.
			TableResult result = queryJob.getQueryResults();

			// Print all pages of the results.
			for (FieldValueList row : result.iterateAll()) {
				String PrimerNombre = row.get("PrimerNombre").getStringValue();
				//long viewCount = row.get("view_count").getLongValue();
				System.out.printf("PrimerNombre: %s\n ", PrimerNombre);
			}
		} catch (Exception ex) {
			System.out.println(ex.toString());
		}
	}
}
