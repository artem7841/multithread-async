using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
    public class SmartClusterClient : ClusterClientBase
    {
        public SmartClusterClient(string[] replicaAddresses) : base(replicaAddresses)
        {
        }

        public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            {
                var partOfTimeout = timeout / ReplicaAddresses.Length;
                var tasksAtWork =  new List<Task<string>>();
                var timeoutGlobal = Task.Delay(timeout).ContinueWith<string>(_ => throw new TimeoutException());
                tasksAtWork.Add(timeoutGlobal);
                
                foreach (var replicaAddress in ReplicaAddresses)
                {
                    var webRequest = CreateRequest(replicaAddress + "?query=" + query);
                    Log.InfoFormat($"Processing {webRequest.RequestUri}");
                    var replicaTask = ProcessRequestAsync(webRequest);
                    var timeoutDelay = Task.Delay(partOfTimeout).ContinueWith<string>(_ => throw new TimeoutException());
                    
                    tasksAtWork.Add(timeoutDelay);
                    tasksAtWork.Add(replicaTask);
                    
                    var taskInCycle = await Task.WhenAny(tasksAtWork);
                    tasksAtWork.Remove(timeoutDelay);

                    if (taskInCycle == timeoutGlobal)
                        throw new  TimeoutException();
                    
                    if (taskInCycle == timeoutDelay)
                        continue;

                    try
                    {
                        return await taskInCycle;
                    }
                    catch (Exception e)
                    {
                        tasksAtWork.Remove(taskInCycle);
                        continue;
                    }
                        
                }
                var resultTask = await Task.WhenAny(tasksAtWork);
                return await resultTask;
            }
        }

        protected override ILog Log => LogManager.GetLogger(typeof(SmartClusterClient));
    }
}
