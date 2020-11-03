using Commons_Core.Helpers.Kafka;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json.Linq;
using Simulator;
using Simulator.REST;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaRESTSimulator
{
    public class Program
    {
        private static string KafkaServer = "192.168.2.93:9092";
        private static string Interface = "NATIVE";
        private static string SchemaRegistryUrl = "192.168.2.93:8081";
        private static int Interval;
        private static int Iterations;
        private static int Option;
        private static string RESTProxyIP;
        private static string RESTProxyPort;

        public static IConfigurationRoot Configuration { get; set; }

        public static async Task Main(string[] args)
        {
            try
            {
                Console.WriteLine("Simulador de publicações em tópicos Kafka\r");

                if (args.Length > 0)
                {
                    Console.WriteLine(string.Join(", ", args));

                    for (int i = 0; i < args.Length; i++)
                    {
                        string value = args[i];

                        switch (value)
                        {
                            case "--kafkaServer":
                                KafkaServer = args[i + 1];
                                break;
                            case "--schemaRegistry":
                                SchemaRegistryUrl = args[i + 1];
                                break;
                            case "--ip":
                                RESTProxyIP = args[i + 1];
                                break;
                            case "--port":
                                RESTProxyPort = args[i + 1];
                                break;
                            case "--option":
                                Option = Convert.ToInt32(args[i + 1]);
                                break;
                            case "--count":
                                Iterations = Convert.ToInt32(args[i + 1]);
                                break;
                            case "--seconds":
                                Interval = Convert.ToInt32(args[i + 1]) * 1000;
                                break;
                            case "--interface":
                                Interface = args[i + 1];
                                break;
                        }
                    }
                }
                else
                {
                    using (StreamReader r = new StreamReader("appsettings.json"))
                    {
                        var appSettings = JObject.Parse(r.ReadToEnd());

                        RESTProxyIP = (string)appSettings["RESTProxyIP"];
                        RESTProxyPort = (string)appSettings["RESTProxyPort"];
                    }

                    Console.WriteLine("Selecione o tópico e pressione Enter:");
                    Console.WriteLine("1-Meteorological-Alerts + Fire-Alerts\n");
                    Console.WriteLine("2-Meteorological-Alerts\n");
                    Console.WriteLine("3-Fire-Alerts\n");
                    Option = Convert.ToInt32(Console.ReadLine());

                    Console.WriteLine("Digite a quantidade de mensagens a serem geradas\n");
                    Iterations = Convert.ToInt32(Console.ReadLine());

                    Console.WriteLine("Digite o intervalo em segundos entre o envio de mensagens:\n");
                    Interval = Convert.ToInt32(Console.ReadLine()) * 1000;

                    //Console.WriteLine("Informe a interface a ser utilizada:\n");
                    //Interface = Console.ReadLine();
                }

                if (Interface == "REST")
                {
                    var FireAlertPublisher = new RestPublisher(RESTProxyIP, RESTProxyPort, "Fire-Alerts");
                    var MeteorologicalAlertPublisher = new RestPublisher(RESTProxyIP, RESTProxyPort, "Meteorological-Alerts");

                    for (int i = 0; i < Iterations; i++)
                    {
                        var fireAlert = new FireAlert().RestJson;
                        var meteorologicalAlert = new MeteorologicalAlert().RestJson;

                        switch (Option)
                        {
                            case 1:
                                FireAlertPublisher.SendMessage(fireAlert);
                                MeteorologicalAlertPublisher.SendMessage(meteorologicalAlert);

                                break;
                            case 2:
                                MeteorologicalAlertPublisher.SendMessage(meteorologicalAlert);

                                break;
                            case 3:
                                FireAlertPublisher.SendMessage(fireAlert);

                                break;
                        }

                        Thread.Sleep(Interval);
                    }
                }
                else
                {
                    var FireAlertPublisher = new JsonProducerHelper<string, string>(KafkaServer, SchemaRegistryUrl);
                    var MeteorologicalAlertPublisher = new JsonProducerHelper<string, string>(KafkaServer, SchemaRegistryUrl);

                    for (int i = 0; i < Iterations; i++)
                    {
                        var fireAlertKey = new FireAlert().Key;
                        var fireAlertValue = new FireAlert().Value;
                        var meteorologicalAlertKey = new MeteorologicalAlert().Key;
                        var meteorologicalAlertValue = new MeteorologicalAlert().Value;

                        switch (Option)
                        {
                            case 1:
                                FireAlertPublisher.PublishAsync(fireAlertKey, fireAlertValue, "Fire-Alerts");
                                MeteorologicalAlertPublisher.PublishAsync(meteorologicalAlertKey, meteorologicalAlertValue, "Meteorological-Alerts");
                                break;

                            case 2:
                                MeteorologicalAlertPublisher.PublishAsync(meteorologicalAlertKey, meteorologicalAlertValue, "Meteorological-Alerts");
                                break;

                            case 3:
                                FireAlertPublisher.PublishAsync(fireAlertKey, fireAlertValue, "Fire-Alerts");
                                break;
                        }

                        await Task.Delay(Interval);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Houve um erro na execução: {ex.Message}");
            }
        }
    }
}
