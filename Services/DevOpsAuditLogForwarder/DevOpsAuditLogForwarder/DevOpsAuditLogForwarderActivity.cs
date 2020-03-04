using System;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.Services.Audit.WebApi;
using Microsoft.VisualStudio.Services.Common;
using Microsoft.VisualStudio.Services.WebApi;
using Newtonsoft.Json;

namespace DevOpsAuditLogForwarder
{
    public static class DevOpsAuditLogForwarderActivity
    {
        [FunctionName("DevOpsAuditLogForwarderActivity")]
        public async static Task Run([TimerTrigger("0 */5 * * * *", RunOnStartup = true)]TimerInfo myTimer, 
                                     [EventHub("audit-logs-devops-lolinc", Connection = "EventHubConnectionString")]IAsyncCollector<string> outputEvents, ExecutionContext context, ILogger log)
        {
            

            var config = context.BuildConfiguraion();

            // Get the audit log client
            var connection = new VssConnection(new Uri(config["AzureDevOpsUrl"]), new VssBasicCredential(string.Empty, config["AzureDevOpsPersonalAccessToken"]));
            var auditClient = await connection.GetClientAsync<AuditHttpClient>();

            // Query the audit log for interval
            int count = 0;
            var now = DateTime.UtcNow;
            var startTime = now.AddMinutes(-10);
            var endTime = now.AddMinutes(-5);
            var logQueryResult = await auditClient.QueryLogAsync(startTime: startTime, endTime: endTime, batchSize: 100, continuationToken: null);
            foreach (var logEvent in logQueryResult.DecoratedAuditLogEntries)
            {
                var logJson = JsonConvert.SerializeObject(logEvent);
                await outputEvents.AddAsync(logJson);
                count++;
            }
            while (logQueryResult.HasMore)
            {
                logQueryResult = await auditClient.QueryLogAsync(batchSize: 100, continuationToken: logQueryResult.ContinuationToken);
                foreach (var logEvent in logQueryResult.DecoratedAuditLogEntries)
                {
                    var logJson = JsonConvert.SerializeObject(logEvent);
                    await outputEvents.AddAsync(logJson);
                    count++;
                }
            }

            log.LogInformation($"Completed audit event forwarding: startTime - {startTime}, endTime - {endTime}, count - {count}");
        }
    }
}
