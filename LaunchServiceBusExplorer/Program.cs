using MessagingSamples;
using System;
using System.Diagnostics;

namespace LaunchServiceBusExplorer
{
    class Program
    {
        static void Main(string[] args)
        {
            var serviceBusAppPath = Sample.pathToServiceBusExporerFolder + @"\ServiceBusExplorer.exe";
            var filterQueue = " / q \"Startswith(Path, '" + Sample.NamePrefix + "') Eq true\" ";
            var filterTopic = "/t \"Startswith(Path, '" + Sample.NamePrefix + "') Eq true\" ";

            Process.Start(serviceBusAppPath, "/c " + Sample.sbConnectionString + " " + filterQueue + " " + filterTopic);
        }
    }
}
