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
            var globalTimeout = Task.Delay(timeout);
            
            foreach (var replicaAddress in ReplicaAddresses)
            {
                var timeoutTask = Task.Delay(partTimeout);
                var webRequest = CreateRequest(replicaAddress + "?query=" + query);
                Log.InfoFormat($"Processing {webRequest.RequestUri}");
                var replicaTask = ProcessRequestAsync(webRequest);
                var resultTask = await Task.WhenAny(replicaTask, timeoutTask);

                if (resultTask == timeoutTask)
                    continue;
                    
                if(resultTask.IsFaulted)
                    continue;
                    
                if (ReplicaAddresses.Last() == replicaAddress)
                    return await replicaTask;
                    
                await resultTask;
            }
            
            var webRequestLast = CreateRequest(ReplicaAddresses.Last() + "?query=" + query);
            Log.InfoFormat($"Processing {webRequestLast.RequestUri}");
            var replicaLastTask = ProcessRequestAsync(webRequestLast);
            var resultLastTask = await Task.WhenAny(replicaLastTask, globalTimeout);
            
            if (resultLastTask == globalTimeout)
            {
                throw new TimeoutException();
            }
            else
            {
                return await replicaLastTask;
            }
        }

        protected override ILog Log => LogManager.GetLogger(typeof(RoundRobinClusterClient));
    }
}
