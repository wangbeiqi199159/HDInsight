using Microsoft.Azure.Management.HDInsight.Job;
using Microsoft.Azure.Management.HDInsight.Job.Models;
using Hyak.Common;

namespace SubmitHDInsightJobDotNet
{
    class Program
    {
        private static HDInsightJobManagementClient _hdiJobManagementClient;

        private const string ExistingClusterName = "beckywang";
        private const string ExistingClusterUri = ExistingClusterName + ".azurehdinsight.net";
        private const string ExistingClusterUsername = "beckywang";
        private const string ExistingClusterPassword = "Beiqi0509*";

        static void Main(string[] args)
        {
            System.Console.WriteLine("The application is running ...");

            var clusterCredentials = new BasicAuthenticationCloudCredentials { Username = ExistingClusterUsername, Password = ExistingClusterPassword };
            _hdiJobManagementClient = new HDInsightJobManagementClient(ExistingClusterUri, clusterCredentials);

            SubmitPigJob();

            System.Console.WriteLine("Press ENTER to continue ...");
            System.Console.ReadLine();
        }

        private static void SubmitPigJob()
        {
            var parameters = new PigJobSubmissionParameters
            {
                Query = "LOGS = LOAD 'wasbs://pigjob@beiqiw.blob.core.windows.net/example/data/sample.log';"
                             + "LEVELS = foreach LOGS generate REGEX_EXTRACT($0, '(TRACE|DEBUG|INFO|WARN|ERROR|FATAL)', 1)  as LOGLEVEL;"
                             + "FILTEREDLEVELS = FILTER LEVELS by LOGLEVEL is not null;"
                             + "GROUPEDLEVELS = GROUP FILTEREDLEVELS by LOGLEVEL;"
                             + "FREQUENCIES = foreach GROUPEDLEVELS generate group as LOGLEVEL, COUNT(FILTEREDLEVELS.LOGLEVEL) as COUNT;"
                             + "RESULT = order FREQUENCIES by COUNT desc;"
                             + "STORE RESULT INTO 'wasbs://pigjob@beiqiw.blob.core.windows.net/user/beckywang/pigjob' using PigStorage(';') ;"

            };

            System.Console.WriteLine("Submitting the Pig job to the cluster...");
            var response = _hdiJobManagementClient.JobManagement.SubmitPigJob(parameters);
            System.Console.WriteLine("Validating that the response is as expected...");
            System.Console.WriteLine("Response status code is " + response.StatusCode);
            System.Console.WriteLine("Validating the response object...");
            System.Console.WriteLine("JobId is " + response.JobSubmissionJsonResponse.Id);
            System.Console.WriteLine(response.ToString());
        }
    }
}

