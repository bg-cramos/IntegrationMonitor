using System.Collections.Generic;

namespace IntegrationMonitor.Models
{
    public class LogViewModel
    {
        public int Cid { get; set; }

        public string Subject { get; set; }

        public string Consumer { get; set; }

        public string ConsumerName { get; set; }

        public string LogFile { get; set; }

        public List<string> LineasLog { get; set; }
    }
}