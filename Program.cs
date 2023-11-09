using System;
using Google.Cloud.BigQuery.V2;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Auth.OAuth2.Flows;
using Google.Apis.Auth.OAuth2.Responses;
using Google.Apis.Bigquery.v2;
using Google.Apis.Util.Store;
using Google.Apis.Services;
using Google.Apis.Bigquery.v2.Data;
using System.Net.WebSockets;
using System.Data;

string jsonKeyFilePath = @"C:\Users\anand\Downloads\client_secret_718576794220-j4qlas50g72o8tinm00nhc81gjtqffm1.apps.googleusercontent.com.json";

string projectId = "gen-visualizer-27802";

var clientId = "718576794220-j4qlas50g72o8tinm00nhc81gjtqffm1.apps.googleusercontent.com";
var clientSecret = "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";

// Load your user credential JSON file
string userCredentialFile = jsonKeyFilePath;

// Create a UserCredential instance using the user credential JSON file
UserCredential credential;

string jsonKeyFilePath2 = @"C:\Users\anand\Downloads\gen-visualizer-27802-12e0ec482b73.json"; // Replace with your service account key file path

//User Credential

using (var stream = new FileStream(jsonKeyFilePath, FileMode.Open, FileAccess.Read))
{
    credential = await GoogleWebAuthorizationBroker.AuthorizeAsync(
        new ClientSecrets
        {
            ClientId = "885256184108-p7vlg5dcjn78g34n9ln9gn7irhglh67d.apps.googleusercontent.com",
            ClientSecret = "alH_wx-EQKzqnR7xbIFJb7Dc"
        },
        new[] { BigqueryService.Scope.Bigquery },
        "user",
        CancellationToken.None,
        new FileDataStore("BigQuery.List")
    );
}

var service = new BigqueryService(new BaseClientService.Initializer()
{
    HttpClientInitializer = credential,
    ApplicationName = "bigQuery"
});

//Service Account Credential
GoogleCredential credential2;

using (var stream = new FileStream(jsonKeyFilePath2, FileMode.Open, FileAccess.Read))
{
    credential2 = GoogleCredential.FromStream(stream)
        .CreateScoped(BigqueryService.Scope.Bigquery);
}

var service2 = new BigQueryClientBuilder
{
    ProjectId = projectId,
    Credential = credential
}.Build(); 

var client = BigQueryClient.Create(projectId, credential2);

//Get correct query
string query = $"select * from sample.test";

BigQueryJob bjob=client.CreateQueryJob(
    sql:query,parameters:null,options:new QueryOptions { UseQueryCache = false });

//wait for the job to complete
bjob = bjob.PollUntilCompleted().ThrowOnAnyError(); 

//Display the result
foreach(BigQueryRow row in client.GetQueryResults(bjob.Reference))
{
    Console.WriteLine($"{row.RawRow.F}");
}

var queryJobConfig = new JobConfigurationQuery
{
    Query = query
};

//Initialize a job
JobsResource j = service.Jobs;

var k = service2.CreateQueryJob(query,null);

//Initiate a query request and plug query in
Google.Apis.Bigquery.v2.Data.QueryRequest qr = new Google.Apis.Bigquery.v2.Data.QueryRequest();
qr.Query = query;
//qr.MaxResults = 10000;

//Create data table to hold response data
DataTable datab = new DataTable();

//counter for loops
int i = 0;

//Execute intial query
QueryResponse resp = new QueryResponse();

int att = 0;
bool NotFinished = true;

while (att <= 10 && NotFinished)
{
    try
    {
        resp = j.Query(qr, projectId).Execute();

        NotFinished = false;

    }
    catch (Exception e)
    {
        Console.WriteLine(e);
        throw;
    }
    Thread.Sleep(5000);

    att += att + 1;
}