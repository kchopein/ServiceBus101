//   
//   Copyright © Microsoft Corporation, All Rights Reserved
// 
//   Licensed under the Apache License, Version 2.0 (the "License"); 
//   you may not use this file except in compliance with the License. 
//   You may obtain a copy of the License at
// 
//   http://www.apache.org/licenses/LICENSE-2.0 
// 
//   THIS CODE IS PROVIDED *AS IS* BASIS, WITHOUT WARRANTIES OR CONDITIONS
//   OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT LIMITATION
//   ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR A
//   PARTICULAR PURPOSE, MERCHANTABILITY OR NON-INFRINGEMENT.
// 
//   See the Apache License, Version 2.0 for the specific language
//   governing permissions and limitations under the License. 

namespace Sessions
{
    using System;
    using System.IO;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.ServiceBus;
    using Microsoft.Azure.ServiceBus.Core;
    using Newtonsoft.Json;

    public class Program : MessagingSamples.Sample
    {
        public async Task Run(string connectionString)
        {
            Console.WriteLine("Press any key to exit the scenario");

            var client1 = this.InitializeReceiver(connectionString, SessionQueueName, ConsoleColor.White);
            var client2 = this.InitializeReceiver(connectionString, SessionQueueName, ConsoleColor.Blue);
            var client3 = this.InitializeReceiver(connectionString, SessionQueueName, ConsoleColor.Green);

            //var nonSessionClient = this.InitializeNonSessionReceiver(connectionString, SessionQueueName, ConsoleColor.Red);

            Task.WaitAny(
               Task.Run(() => Console.ReadKey()),
               Task.Delay(TimeSpan.FromSeconds(30)));

            await Task.WhenAll(
                this.SendMessagesAsync(Guid.NewGuid().ToString(), connectionString, SessionQueueName));
            //this.SendMessagesAsync(Guid.NewGuid().ToString(), connectionString, SessionQueueName),
            //this.SendMessagesAsync(Guid.NewGuid().ToString(), connectionString, SessionQueueName),
            //this.SendMessagesAsync(Guid.NewGuid().ToString(), connectionString, SessionQueueName),
            //this.SendMessagesAsync(Guid.NewGuid().ToString(), connectionString, SessionQueueName),
            //this.SendMessagesAsync(Guid.NewGuid().ToString(), connectionString, SessionQueueName),
            //this.SendMessagesAsync(Guid.NewGuid().ToString(), connectionString, SessionQueueName),
            //this.SendMessagesAsync(Guid.NewGuid().ToString(), connectionString, SessionQueueName));



            Console.ReadKey();

            await client1.CloseAsync();
            await client2.CloseAsync();
            await client3.CloseAsync();

            Console.ReadKey();
        }

        async Task SendMessagesAsync(string sessionId, string connectionString, string queueName)
        {
            var sender = new MessageSender(connectionString, queueName);

            dynamic data = new[]
            {
                new {step = 1, title = "Shop"},
                new {step = 2, title = "Unpack"},
                new {step = 3, title = "Prepare"},
                new {step = 4, title = "Cook"},
                new {step = 5, title = "Eat"},
            };

            for (int i = data.Length - 1; i > 0 ; i--)
            {
                var message = new Message(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(data[i])))
                {
                    
                    ContentType = "application/json",
                    Label = "RecipeStep",
                    MessageId = i.ToString(),
                    TimeToLive = TimeSpan.FromMinutes(2)
                };
                await sender.SendAsync(message);
                lock (Console.Out)
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine("Message sent: Session {0}, MessageId = {1}", message.SessionId, message.MessageId);
                    Console.ResetColor();
                }
            }

            var independentMessage = new Message(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(new { step = 1000, title = "Independent" })))
            {
                SessionId = new Random(DateTime.UtcNow.Millisecond).NextDouble().ToString(),
                ContentType = "application/json",
                Label = "JAja",
                TimeToLive = TimeSpan.FromMinutes(2)
            };
            await sender.SendAsync(independentMessage);
            lock (Console.Out)
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine("Independent message");
                Console.ResetColor();
            }
        }

        QueueClient InitializeNonSessionReceiver(string connectionString, string queueName, ConsoleColor color)
        {
            var nonSessionClient = new QueueClient(connectionString, queueName, ReceiveMode.PeekLock);
            nonSessionClient.RegisterMessageHandler(async (message, cancellationToken) =>
            {
                lock (Console.Out)
                {
                    Console.ForegroundColor = color;
                    Console.WriteLine("\t\t\t\tIndependent Message received:  \n\t\t\t\t\t\tSessionId = {0}, \n\t\t\t\t\t\tMessageId = {1}, \n\t\t\t\t\t\tSequenceNumber = {2}",
                        message.SessionId,
                        message.MessageId,
                        message.SystemProperties.SequenceNumber);
                    Console.ResetColor();
                }

                await nonSessionClient.CompleteAsync(message.SystemProperties.LockToken);
            },
            new MessageHandlerOptions(e => LogMessageHandlerException(e))
            {
                MaxConcurrentCalls = 1,
                AutoComplete = false,
                MaxAutoRenewDuration = TimeSpan.FromSeconds(15)
            }
             );

            return nonSessionClient;

        }

        QueueClient InitializeReceiver(string connectionString, string queueName, ConsoleColor color)
        {
            var sessionClient = new QueueClient(connectionString, queueName, ReceiveMode.PeekLock);
            sessionClient.RegisterSessionHandler(
               async (session, message, cancellationToken) =>
                {
                    if (message.Label != null &&
                        message.ContentType != null &&
                        message.ContentType.Equals("application/json", StringComparison.InvariantCultureIgnoreCase))
                    {
                        var body = message.Body;

                        dynamic recipeStep = JsonConvert.DeserializeObject(Encoding.UTF8.GetString(body));
                        lock (Console.Out)
                        {
                            Console.ForegroundColor = color;
                            Console.WriteLine(
                                "\t\t\t\tMessage received:  \n\t\t\t\t\t\tSessionId = {0}, \n\t\t\t\t\t\tMessageId = {1}, \n\t\t\t\t\t\tSequenceNumber = {2}," +
                                "\n\t\t\t\t\t\tContent: [ step = {3}, title = {4} ]",
                                message.SessionId,
                                message.MessageId,
                                message.SystemProperties.SequenceNumber,
                                recipeStep.step,
                                recipeStep.title);
                            Console.ResetColor();
                        }
                        await session.CompleteAsync(message.SystemProperties.LockToken);

                        if (recipeStep.step == 5)
                        {
                            // end of the session!
                            await session.CloseAsync();
                        }
                    }
                    else
                    {
                        await session.DeadLetterAsync(message.SystemProperties.LockToken);//, "BadMessage", "Unexpected message");
                    }
                },
                new SessionHandlerOptions(e => LogMessageHandlerException(e))
                {
                    MessageWaitTimeout = TimeSpan.FromSeconds(5),
                    MaxConcurrentSessions = 1,
                    AutoComplete = false,
                    MaxAutoRenewDuration = TimeSpan.FromSeconds(15)
                });
            return sessionClient;
        }

        private Task handleMessage(Message arg1, CancellationToken arg2)
        {
            throw new NotImplementedException();
        }

        private Task LogMessageHandlerException(ExceptionReceivedEventArgs e)
        {
            Console.WriteLine("Exception: \"{0}\" {1}", e.Exception.Message, e.ExceptionReceivedContext.EntityPath);
            return Task.CompletedTask;
        }

       public static int Main(string[] args)
        {
            try
            {
                var app = new Program();
                app.RunSample(args, app.Run);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
                return 1;
            }
            return 0;
        }
    }
}
