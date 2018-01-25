/********************************************************
*                                                       *
*   Copyright (C) Microsoft. All rights reserved.       *
*                                                       *
********************************************************/

namespace MPI
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading;

    public static class SpanTimer
    {
        private static MPI.Intracommunicator comm;
        private static ConcurrentDictionary<string,ActivityTimer> activityTimers = new ConcurrentDictionary<string, ActivityTimer>();
        private static object recordLock = new object();
        private static List<SpanRecord> records;
        public delegate void SpanTimerEventHandler(int processId, string type, DateTime time, bool isStart);
        public static event SpanTimerEventHandler Logging;
        public static double SamplingIntervalInMilliseconds { get; set; } = 1000 * 60;  // one minute

        public static void Initialise(MPI.Intracommunicator c, bool preserveTimeline = false)
        {
            comm = c;
            if (preserveTimeline) records = new List<SpanRecord>();
        }

        /// <summary>
        /// Create a span record for the given span type.  Must have a corresponding Leave.
        /// </summary>
        /// <param name="spanType"></param>
        public static void Enter(string spanType)
        {
            if (comm == null)
                return;
            var time = DateTime.UtcNow;
            ActivityTimer timer;
            if (!activityTimers.TryGetValue(spanType, out timer))
            {
                timer = new ActivityTimer();
                activityTimers.TryAdd(spanType, timer);
            }
            timer.Enter(time);
            if (records != null)
            {
                var spanRecord = new SpanRecord(comm.Rank, spanType, time.Ticks, isStart: true);
                lock (recordLock)
                {
                    records.Add(spanRecord);
                }
            }
            Logging?.Invoke(comm.Rank, spanType, time, true);
        }

        /// <summary>
        /// Complete a span record for the given span type.  Must have previously been Entered.
        /// </summary>
        /// <param name="spanType"></param>
        public static void Leave(string spanType)
        {
            if (comm == null)
                return;
            var time = DateTime.UtcNow;
            ActivityTimer timer;
            if (activityTimers.TryGetValue(spanType, out timer))
            {
                timer.Leave(time);
            }
            if (records != null)
            {
                var spanRecord = new SpanRecord(comm.Rank, spanType, time.Ticks, isStart: false);
                lock (recordLock)
                {
                    records.Add(spanRecord);
                }
            }
            Logging?.Invoke(comm.Rank, spanType, time, false);
        }

        public static double GetPercentTimeInActivity(string spanType)
        {
            ActivityTimer timer;
            if (!activityTimers.TryGetValue(spanType, out timer)) return 0.0;
            return timer.PercentTimeInActivity;
        }

        public static void SaveToHtml(string outputPath = "Timeline.html")
        {
            if (SpanTimer.comm == null)
                return;
            const int root = 0;
            MPI.Intracommunicator comm = SpanTimer.comm;
            // temporarily disable logging
            SpanTimer.comm = null;
            var allRecords = comm.Gather(records, root);
            if (comm.Rank == root)
            {
                SaveToHtml(outputPath, allRecords, allowOpenSpans: false);
            }
            // re-enable logging
            SpanTimer.comm = comm;
        }

        public static void SaveToHtml(string outputPath, string inputPath)
        {
            List<List<SpanRecord>> spanRecordsPerProcess = new List<List<SpanRecord>>();
            using (var reader = new StreamReader(inputPath))
            {
                while (true)
                {
                    string line = reader.ReadLine();
                    if (line == null)
                        break;
                    string[] parts = line.Split('\t');
                    if (parts.Length < 4)
                        continue;
                    if (parts[2] != "Enter" && parts[2] != "Leave")
                        continue;
                    DateTime time = DateTime.ParseExact(parts[0], "o", CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
                    int process = int.Parse(parts[1]);
                    while (spanRecordsPerProcess.Count <= process)
                    {
                        spanRecordsPerProcess.Add(new List<SpanRecord>());
                    }
                    bool isStart = parts[2] == "Enter";
                    string type = parts[3];
                    spanRecordsPerProcess[process].Add(new SpanRecord(process, type, time.Ticks, isStart));
                } 
            }
            SaveToHtml(outputPath, spanRecordsPerProcess, allowOpenSpans: true);
        }

        #region Helper methods

        private static void SaveToHtml(string outputPath, IReadOnlyList<List<SpanRecord>> spanRecordsPerProcess, bool allowOpenSpans)
        {
            var spansPerProcess = ComputeSpans(spanRecordsPerProcess, allowOpenSpans);
            var maxProcessLength = spansPerProcess.Max(kvp => kvp.Value.Sum(span => span.Length));
            var minSpanLength = double.MaxValue;

            var styleElement = new StringBuilder();
            var columns = new List<string>();
            foreach (var kvp in spansPerProcess)
            {
                var column = new StringBuilder();
                uint nextId = 0;
                double error = 0;
                foreach (var span in kvp.Value)
                {
                    double desired = (double)span.Length / maxProcessLength + error;
                    double actual = desired;
                    error = desired - actual;
                    if (actual > 0)
                    {
                        styleElement.AppendLine($"#id{kvp.Key}_{nextId} {{ height: {actual * 100}%; }}");
                        column.AppendLine($"<div class=\"{span.Type}\" id=\"id{kvp.Key}_{nextId++}\"></div>");
                        if (actual < minSpanLength)
                            minSpanLength = actual;
                    }
                }

                columns.Add(column.ToString());
            }

            var colorMap = ComputeColorMap(spansPerProcess);
            var minFraction = minSpanLength;
            WriteOut(outputPath, columns, styleElement.ToString(), colorMap, minFraction);
        }

        private static IDictionary<int, List<Span>> ComputeSpans(IReadOnlyList<List<SpanRecord>> spanRecordsPerProcess, bool allowOpenSpans)
        {
            long earliestTime = spanRecordsPerProcess.Min(list => list[0].Time);
            long latestTime = spanRecordsPerProcess.Max(list => list[list.Count-1].Time);

            var spansPerProcess = new SortedDictionary<int, List<Span>>();
            for (int processId = 0; processId < spanRecordsPerProcess.Count; processId++) checked
            {
                var spanRecordsForProcess = spanRecordsPerProcess[processId];
                var spansForProcess = new List<Span>();
                var stack = new Stack<SpanRecord>();
                SpanRecord lastSpanRecord = null;
                foreach (var currentSpanRecord in spanRecordsForProcess)
                {
                    if (currentSpanRecord.IsStart)
                    {
                        if (lastSpanRecord != null)
                        {
                            var spanType = stack.Count == 0 ? "None" : stack.Peek().Type;
                            spansForProcess.Add(new Span(currentSpanRecord.Time - lastSpanRecord.Time, spanType));
                        }
                        else if (currentSpanRecord.Time > earliestTime)
                        {
                            var spanType = "None";
                            spansForProcess.Add(new Span(currentSpanRecord.Time - earliestTime, spanType));
                        }

                        stack.Push(currentSpanRecord);
                    }
                    else
                    {
                        if (stack.Count == 0)
                        {
                            throw new Exception($"The end span record does not have a corresponding start span record.{System.Environment.NewLine}{currentSpanRecord}");
                        }

                        if (lastSpanRecord == null)
                        {
                            throw new Exception($"Span timer implementation error. {System.Environment.NewLine}{currentSpanRecord}");
                        }

                        var popped = stack.Pop();
                        if (popped.Type != currentSpanRecord.Type)
                        {
                            throw new Exception($"Span type mismatch.{System.Environment.NewLine}{popped}{System.Environment.NewLine}{currentSpanRecord}");
                        }

                        spansForProcess.Add(new Span(currentSpanRecord.Time - lastSpanRecord.Time, currentSpanRecord.Type));
                    }

                    lastSpanRecord = currentSpanRecord;
                }

                if (stack.Count != 0)
                {
                    var popped = stack.Pop();
                    if (allowOpenSpans)
                    {
                        spansForProcess.Add(new Span(latestTime - lastSpanRecord.Time, popped.Type));
                    }
                    else
                    {
                        StringBuilder sb = new StringBuilder();
                        while (stack.Count > 0)
                            sb.AppendLine(stack.Pop().ToString());
                        string suffix = (sb.Length > 0) ? "Remaining stack: " + sb : "";
                        throw new Exception($"{popped} has no matching End. {suffix}");
                    }
                }

                spansPerProcess.Add(processId, spansForProcess);
            }

            return spansPerProcess;
        }

        private static Dictionary<string, string> ComputeColorMap(IDictionary<int, List<Span>> spansPerProcess)
        {
            // http://www.w3schools.com/cssref/css_colors.asp
            var colors = new[] { "DodgerBlue", "DarkSalmon", "lime", "orchid", "red", "yellow", "aqua", "silver", "YellowGreen", "fuchsia", "orange", "AntiqueWhite" };
            var spanTypes = spansPerProcess.SelectMany(kvp => kvp.Value.Select(s => s.Type)).Distinct().ToList();
            spanTypes.Remove("None");
            if (spanTypes.Count > colors.Length)
            {
                throw new Exception($"Not enough predefined colours in palette. Need to add {checked(spanTypes.Count - colors.Length)} more.");
            }
            // Sort to stabilize the color assignment.
            spanTypes.Sort();
            var result = spanTypes.Select((x, i) => new {Type = x, Index = i}).ToDictionary(p => p.Type, p => colors[p.Index]);
            result.Add("None", "white");
            return result;
        } 

        private static void WriteOut(string path, List<string> columns, string styleElement, Dictionary<string, string> colorMap, double minFraction)
        {
            using (var writer = new StreamWriter(path))
            {
                writer.WriteLine("<!DOCTYPE html>");
                writer.WriteLine("<html>");
                writer.WriteLine("<head>");
                writer.WriteLine("<style>");
                writer.WriteLine("div { border: 0px solid gray; }");
                foreach (var typeColor in colorMap)
                {
                    writer.WriteLine($"div.{typeColor.Key} {{ background: {typeColor.Value}; }}");
                }
                
                writer.WriteLine("th { vertical-align: top; }");
                // the best height makes the height of the smallest span equal to 1 pixel.
                int maxHeight = 10000;
                int minHeight = 100;
                if (minFraction <= 0)
                    minFraction = 1e-4;
                var desiredHeight = Math.Ceiling(1 / minFraction);
                int height = (int)Math.Max(Math.Min(desiredHeight, maxHeight), minHeight);
                writer.WriteLine($"th.timeline {{ height: {height}px }}");
                writer.Write(styleElement);
                writer.WriteLine("</style>");
                writer.WriteLine("</head>");
                writer.WriteLine("<body>");
                writer.WriteLine("<table>");
                foreach (var typeColor in colorMap)
                {
                    writer.WriteLine($"<tr><th colspan=2><div class=\"{typeColor.Key}\">{typeColor.Key}</div></th></tr>");
                }

                writer.WriteLine("<tr>");
                for (int process = 0; process < columns.Count; process++)
                {
                    writer.WriteLine($"<th>Process {process}</th>");
                }

                writer.WriteLine("</tr>");
                writer.WriteLine("<tr>");
                for (int process = 0; process < columns.Count; process++)
                {
                    writer.WriteLine($"<th class=\"timeline\">");
                    writer.Write(columns[process]);
                    writer.WriteLine("</th>");
                }

                writer.WriteLine("</tr>");
                writer.WriteLine("</table>");
                writer.WriteLine("</body>");
                writer.WriteLine("</html>");
            }
        }

        #endregion

        #region Nested classes

        [Serializable]
        private class SpanRecord
        {
            public SpanRecord(int processId, string type, long time, bool isStart)
            {
                this.ProcessId = processId;
                this.Type = type;
                this.Time = time;
                this.IsStart = isStart;
            }

            private SpanRecord(string spanRecord)
            {
                var tokens = spanRecord.Split("\t".ToCharArray());
                if (tokens.Length != 4)
                {
                    throw new Exception($"Badly formatted span record: {spanRecord}");
                }

                this.ProcessId = Convert.ToInt32(tokens[0]);
                this.Type = tokens[1];
                this.Time = long.Parse(tokens[2]);
                this.IsStart = Convert.ToBoolean(tokens[3]);
            }

            public int ProcessId { get; }

            public string Type { get; }

            public long Time { get; }
            
            public bool IsStart { get; }

            public override string ToString()
            {
                return $"SpanRecord({this.ProcessId}, {this.Type}, {this.Time}, {this.IsStart})";
            }
        }

        private class Span
        {
            public Span(long length, string type)
            {
                this.Length = length;
                this.Type = type;
            }

            public long Length { get; }

            public string Type { get; }

            public override string ToString()
            {
                return $"{this.Length}\t{this.Type}";
            }
        }

        private class ActivityTimer
        {
            DateTime enterTime;
            DateTime lastReset;
            double millisecondsInActivity;
            double fractionTimeInActivity;
            public double PercentTimeInActivity { get { return fractionTimeInActivity * 100; } }

            private void Update()
            {
                DateTime now = DateTime.UtcNow;
                double millisecondsSinceReset = (lastReset == default(DateTime)) ? double.PositiveInfinity : (now - lastReset).TotalMilliseconds;
                if (millisecondsSinceReset > SamplingIntervalInMilliseconds)
                {
                    fractionTimeInActivity = millisecondsInActivity / millisecondsSinceReset;
                    lastReset = now;
                    millisecondsInActivity = 0;
                }
            }

            public void Enter(DateTime time)
            {
                enterTime = time;
            }

            public void Leave(DateTime time)
            {
                double milliseconds = (time - enterTime).TotalMilliseconds;
                millisecondsInActivity += milliseconds;
                Update();
            }
        }

        #endregion
    }
}
