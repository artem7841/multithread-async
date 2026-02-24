using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
    public class RoundRobinClusterClient : ClusterClientBase
    {
        public RoundRobinClusterClient(string[] replicaAddresses) : base(replicaAddresses)
        {
        }

        public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            
            var partTimeout = timeout / ReplicaAddresses.Length;
            
            foreach (var replicaAddress in ReplicaAddresses)
            {
                try
                {
                    var timeoutTask = Task.Delay(partTimeout);
                    var webRequest = CreateRequest(replicaAddress + "?query=" + query);
                    Log.InfoFormat($"Processing {webRequest.RequestUri}");
                    var replicaTask = ProcessRequestAsync(webRequest);
                    var resultTask = await Task.WhenAny(replicaTask, timeoutTask);

                    if (resultTask == timeoutTask)
                        continue;
                    else
                    {
                        if (ReplicaAddresses.Last() == replicaAddress)
                            return await replicaTask;
                        await resultTask;
                        continue;
                    }
                }
                catch (Exception e)
                {
                    continue;
                }
            }
            
            throw new TimeoutException();
        }

        protected override ILog Log => LogManager.GetLogger(typeof(RoundRobinClusterClient));
    }
}
