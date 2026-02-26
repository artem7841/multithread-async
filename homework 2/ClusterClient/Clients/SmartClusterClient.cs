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
        private readonly Dictionary<string, Queue<double>> stats = new Dictionary<string, Queue<double>>();
        private readonly object lockObj = new object();

        public SmartClusterClient(string[] replicaAddresses) : base(replicaAddresses)
        {
            foreach (var address in replicaAddresses)
                stats[address] = new Queue<double>();
        }

        public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            {
                var orderedReplicas = ReplicaAddresses
                    .OrderBy(GetAverage)
                    .ToArray();
                var partOfTimeout = timeout / orderedReplicas.Length;
                var tasksAtWork = new List<(Task<string> task, string replica, DateTime startTime)>();
                var timeoutGlobal = Task.Delay(timeout)
                    .ContinueWith<string>(_ => throw new TimeoutException());
                var tasks = new List<Task<string>> { timeoutGlobal };
                
                foreach (var replicaAddress in orderedReplicas)
                {
                    var webRequest = CreateRequest(replicaAddress + "?query=" + query);
                    Log.InfoFormat($"Processing {webRequest.RequestUri}");
                    var startTime = DateTime.UtcNow;
                    var replicaTask = ProcessRequestAsync(webRequest);

                    tasksAtWork.Add((replicaTask, replicaAddress, startTime));
                    tasks.Add(replicaTask);

                    var timeoutDelay = Task.Delay(partOfTimeout).ContinueWith<string>(_ => throw new TimeoutException());

                    tasks.Add(timeoutDelay);

                    var taskInCycle = await Task.WhenAny(tasks);

                    tasks.Remove(timeoutDelay);

                    if (taskInCycle == timeoutGlobal)
                        throw new TimeoutException();
                    
                    if (taskInCycle == timeoutDelay)
                        continue;

                    try
                    {
                        var result = await taskInCycle;
                        var info = tasksAtWork.First(t => t.task == taskInCycle);
                        UpdateStatistics(info.replica, DateTime.UtcNow - info.startTime);

                        return result;
                    } catch
                    {
                        tasks.Remove(taskInCycle);
                        tasksAtWork.RemoveAll(t => t.task == taskInCycle);
                    }
                }
                while (tasksAtWork.Count > 1)
                {
                    var completedTask = await Task.WhenAny(tasks);

                    if (completedTask == timeoutGlobal)
                        throw new TimeoutException();

                    try
                    {
                        var result = await completedTask;

                        var info = tasksAtWork.First(t => t.task == completedTask);
                        UpdateStatistics(info.replica, DateTime.UtcNow - info.startTime);

                        return result;
                    }
                    catch
                    {
                        tasks.Remove(completedTask);
                        tasks.RemoveAll(t => t == completedTask);
                    }
                }
            }
            throw new TimeoutException();
        }

        protected override ILog Log => LogManager.GetLogger(typeof(SmartClusterClient));
        
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
    }
}
