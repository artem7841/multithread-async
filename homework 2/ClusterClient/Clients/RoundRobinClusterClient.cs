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
        private readonly Dictionary<string, Queue<double>> stats = new ();
        private readonly object lockObj = new ();
        
        public RoundRobinClusterClient(string[] replicaAddresses) : base(replicaAddresses)
        {
            foreach (var address in replicaAddresses)
                stats[address] = new Queue<double>();
        }

        public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            var orderedReplicas = ReplicaAddresses
                .OrderBy(GetAverage)
                .ToArray();

            var partTimeout = timeout / orderedReplicas.Length;
            var globalTimeout = Task.Delay(timeout);
            
            foreach (var replicaAddress in orderedReplicas)
            {
                var startTime = DateTime.UtcNow;
                var timeoutTask = Task.Delay(partTimeout);
                var webRequest = CreateRequest(replicaAddress + "?query=" + query);
                Log.InfoFormat($"Processing {webRequest.RequestUri}");
                var replicaTask = ProcessRequestAsync(webRequest);
                var resultTask = await Task.WhenAny(replicaTask, timeoutTask);

                if (resultTask == timeoutTask)
                    continue;
                    
                if(resultTask.IsFaulted)
                    continue;
                
                var result = await replicaTask;
                UpdateStatistics(replicaAddress, DateTime.UtcNow - startTime);
                return result;
            }
            
            var lastReplica = orderedReplicas.Last();
            var webRequestLast = CreateRequest(lastReplica + "?query=" + query);
            Log.InfoFormat($"Processing {webRequestLast.RequestUri}");

            var startLast = DateTime.UtcNow;
            var replicaLastTask = ProcessRequestAsync(webRequestLast);

            var resultLastTask = await Task.WhenAny(replicaLastTask, globalTimeout);

            if (resultLastTask == globalTimeout)
                throw new TimeoutException();

            var resultFinal = await replicaLastTask;
            UpdateStatistics(lastReplica, DateTime.UtcNow - startLast);
            return resultFinal;
        }
        
        private double GetAverage(string replica)
        {
            lock (lockObj)
            {
                var queue = stats[replica];
                return queue.Count == 0 ? double.MaxValue : queue.Average();
            }
        }
        
        private void UpdateStatistics(string replica, TimeSpan time)
        {
            lock (lockObj)
            {
                var queue = stats[replica];
                queue.Enqueue(time.TotalMilliseconds);
            }
        }

        protected override ILog Log => LogManager.GetLogger(typeof(RoundRobinClusterClient));
    }
}
