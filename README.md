# Run Pig jobs using Visual Studio and C# (.NET SDK) in Azure HDInsight
## Prerequisites

To complete the steps in this article, you need the following:
  
* Azure subscription or [free trial account](https://azure.microsoft.com/en-us/free/)

* Visual Studio 2015 Community. (click [here](https://www.visualstudio.com/downloads/) to download)

* Azure SDK 2.9.5 or later. (click [here](https://www.microsoft.com/en-us/download/details.aspx?id=54289) to download)

## Create an HDInsight cluster
  * Sign in to the [Azure portal](https://portal.azure.com/).

  * Select `NEW` on the menu.

[![2.png](https://s18.postimg.org/hyu7el4zd/image.png)](https://postimg.org/image/v2zrr9x11/)

  * Select `Intelligence + analytics`, then select `HDInsight`.

[![1.png](https://s14.postimg.org/nmtgy0r1d/image.png)](https://postimg.org/image/y9na3fz6l/)

  * Fill the `Cluster Name`, choose your `Subscription`. In `Cluster Configuration`, choose `Cluster Type: Hadoop`, `Operation System : Windows`.

[![3.png](https://s22.postimg.org/yv8arb4z5/image.png)](https://postimg.org/image/z7zoxhn8t/)

  * Under `Credentials`, create your cluster login username and password, then choose `YES` under `Enable Remote Desktop`. 
Create `Data Source`, do remember your `Storage Account Name` and `Container Name`. Choose `Cluster Size`, create `Resource group`, then click `Create`.

[![5.png](https://s32.postimg.org/com31b7k5/image.png)](https://postimg.org/image/bmbwiroqp/)

  * Once the development of your cluster finished, you can see the `Overview` page of the cluster.

[![QQ图片20170213143223.png](https://s2.postimg.org/a0wr1bgax/QQ_20170213143223.png)](https://postimg.org/image/3n7ny2bet/)

(For detailed information about managing Windows-based Hadoop clusters in HDInsight, see [Microsoft Documentation](https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-administer-use-management-portal#connect-to-clusters-using-rdp))
## Submit a pig job script to the cluster and transfer the results to a blob storage

  * In Visual Studio, click `Tool` to connect to your Azure subscription. If your installed Azure SDK successfully, you'll see the cluster you created under HDInsight.

[![8.png](https://s28.postimg.org/5hquud9j1/image.png)](https://postimg.org/image/6wsfj3am1/)

  * Create the application using Visual Studio.

    From the `File` menu in Visual Studio, select`New` and then select `Project`.

    For the new project, type or select the following values.

    **_Property	: Value_**

    **_Category	: Templates/Visual C#/Windows_**

    **_Template	: Console Application_**

    **_Name : PigJob_**
    
    Click `OK` to create the project.

[![6.png](https://s2.postimg.org/3xblhe0c9/image.png)](https://postimg.org/image/wzpvk7mlx/)

  * From the `Tools` menu, select `Library Package Manager` or `Nuget Package Manage`r, and then select `Package Manager Console`.

    Run the following command in the console to install the .NET SDK packages.
```
 Install-Package Microsoft.Azure.Management.HDInsight.Job
```
  * From Solution Explorer, double-click Program.cs to open it. Replace the existing code with the following. (Remember to replace `<Your HDInsight Cluster Name>` to your cluster's name, replace `<Cluster Username>` to your cluster user name, and  replace `<Cluster User Password>` to your cluster user password.

```
 using Microsoft.Azure.Management.HDInsight.Job;
 using Microsoft.Azure.Management.HDInsight.Job.Models;
 using Hyak.Common;

 namespace SubmitHDInsightJobDotNet
 {
     class Program
     {
         private static HDInsightJobManagementClient _hdiJobManagementClient;

         private const string ExistingClusterName = "beckywang"; // <Your HDInsight Cluster Name>
         private const string ExistingClusterUri = ExistingClusterName + ".azurehdinsight.net";
         private const string ExistingClusterUsername = "beckywang";  // <Cluster Username>
         private const string ExistingClusterPassword = "<some complicated password>";  // <Cluster User Password>

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
               // pig query
              {
                 Query = "LOGS = LOAD 'wasbs://pigjob@beiqiw.blob.core.windows.net/example/data/sample.log';" // <container name>@<storage account name>.blob.core.windows.net/example/data/sample.log
                             + "LEVELS = foreach LOGS generate REGEX_EXTRACT($0, '(TRACE|DEBUG|INFO|WARN|ERROR|FATAL)', 1)  as LOGLEVEL;"
                             + "FILTEREDLEVELS = FILTER LEVELS by LOGLEVEL is not null;"
                             + "GROUPEDLEVELS = GROUP FILTEREDLEVELS by LOGLEVEL;"
                             + "FREQUENCIES = foreach GROUPEDLEVELS generate group as LOGLEVEL, COUNT(FILTEREDLEVELS.LOGLEVEL) as COUNT;"
                             + "RESULT = order FREQUENCIES by COUNT desc;"
                              // Store result into blob container
                             + "STORE RESULT INTO 'wasbs://pigjob@beiqiw.blob.core.windows.net/user/beckywang/pigjob' using PigStorage(';') ;"
             };

             System.Console.WriteLine("Submitting the Pig job to the cluster...");
             var response = _hdiJobManagementClient.JobManagement.SubmitPigJob(parameters);
             System.Console.WriteLine("Validating that the response is as expected...");
             System.Console.WriteLine("Response status code is " + response.StatusCode);
             System.Console.WriteLine("Validating the response object...");
             System.Console.WriteLine("JobId is " + response.JobSubmissionJsonResponse.Id);
         }
     }
 }

```

  * Press F5 to start the application.

[![7.png](https://s24.postimg.org/yh8e1h739/image.png)](https://postimg.org/image/ld2tosf1d/)

  The Pig job is submitted to the cluster successfully.

  * Press ENTER to exit the application.

  * You can find the result file in Storage Account. Go back to Azure Portal, click `Storage Account`, click your storage account name, click `Overview`, then click `Blobs` .

[![11.png](https://s31.postimg.org/49wnxiumj/image.png)](https://postimg.org/image/49wnxiumf/)

  * Click the container name, you can find the result file named 'part-r-00000' under path `user/beckywang/pigjob`

[![12.png](https://s2.postimg.org/484afloxl/image.png)](https://postimg.org/image/8h90hrs6t/)

  * Here are the screen shots of the result.
 
[![14.png](https://s12.postimg.org/t5d1kn14t/image.png)](https://postimg.org/image/3mkp7mhkp/)

## Deleting the HDInsight cluster

To delete the cluster, click **Delete** under the cluster Overview page. Then click **Yes**.
[![QQ图片20170213150052.png](https://s31.postimg.org/ynrdtkdbf/QQ_20170213150052.png)](https://postimg.org/image/hzzvr2ijr/)
