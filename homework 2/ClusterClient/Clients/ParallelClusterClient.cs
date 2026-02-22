using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
    public class ParallelClusterClient : ClusterClientBase
    {
        public ParallelClusterClient(string[] replicaAddresses) : base(replicaAddresses)
        {
        }

        public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            var timeoutTask = Task.Delay(timeout).ContinueWith<string>(_ => throw new TimeoutException());
            
            var uris = ReplicaAddresses.Select(async uri =>
            {
                var webRequest = CreateRequest(uri + "?query=" + query);
                Log.InfoFormat($"Processing {webRequest.RequestUri}");
                var resultTask = await ProcessRequestAsync(webRequest);
                return resultTask;

            }).ToList();
            uris.Add(timeoutTask);

            while (uris.Any())
            {
                try
                {
                    var resultTask = await Task.WhenAny(uris);
                    if (resultTask == timeoutTask)
                        await timeoutTask; 
                    return await resultTask;
                }
                catch (Exception e)
                {
                    if (uris.Count == 2) 
                        throw;
                    else
                        uris.Remove(uris.First());
                }
            }
             throw new TimeoutException();
        }

        protected override ILog Log => LogManager.GetLogger(typeof(ParallelClusterClient));
    }
}
