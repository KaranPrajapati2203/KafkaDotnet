using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using System;
using System.Threading;
using System.Threading.Tasks;

class Program
{
    public static async Task Main(string[] args)
    {
        var configuration = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .Build();

        var config = new ConsumerConfig
        {
            BootstrapServers = $"{configuration["BootstrapService:Server"]}:{configuration["BootstrapService:Port"]}",
            GroupId = "test-consumer-group",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
        {
            consumer.Subscribe(configuration["BootstrapService:Topic"]);

            CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true;
                cancellationTokenSource.Cancel();
            };

            try
            {
                while (true)
                {
                    var consumeResult = consumer.Consume(cancellationTokenSource.Token);

                    var message = consumeResult.Message.Value;
                    var dataModel = JsonConvert.DeserializeObject<DataModel>(message);

                    if (dataModel.Counter % 2 == 0)
                    {
                        Console.WriteLine($"Consumed message with even counter: {message}");
                    }
                    //else
                    //{
                    //    Console.WriteLine($"Skipped message with odd counter: {message}");
                    //}
                }
            }
            catch (OperationCanceledException)
            {
                // Ensure the consumer leaves the group cleanly and final offsets are committed.
                consumer.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error consuming message: {ex.Message}");
            }
        }
    }
}

public class DataModel
{
    public int Counter { get; set; }
    public DateTime Timestamp { get; set; }
    public string Content { get; set; }
}
