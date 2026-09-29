using System;
using NATS.Client;

namespace IntegrationMonitor
{
    public static class NatsMonitor
    {
        private static IConnection _connection;

        private static readonly object _lock =
            new object();

        public static bool Connected
        {
            get
            {
                lock (_lock)
                {
                    return _connection != null &&
                           _connection.State ==
                           ConnState.CONNECTED;
                }
            }
        }

        public static string Status
        {
            get
            {
                return Connected
                    ? "CONNECTED"
                    : "DISCONNECTED";
            }
        }

        public static string LastError
        {
            get;
            private set;
        }

        public static DateTime? ConnectedAt
        {
            get;
            private set;
        }

        public static void Start()
        {
            lock (_lock)
            {
                if (_connection != null)
                {
                    return;
                }

                try
                {
                    Options options =
                        ConnectionFactory
                            .GetDefaultOptions();

                    options.Url =
                        "nats://localhost:4222";

                    options.Name =
                        "IntegrationMonitor";

                    options.DisconnectedEventHandler +=
                        (sender, args) =>
                        {
                            LastError =
                                "NATS desconectado";
                        };

                    options.ReconnectedEventHandler +=
                        (sender, args) =>
                        {
                            LastError = null;

                            ConnectedAt =
                                DateTime.Now;
                        };

                    options.ClosedEventHandler +=
                        (sender, args) =>
                        {
                            LastError =
                                "Conexión NATS cerrada";
                        };

                    options.AsyncErrorEventHandler +=
                        (sender, args) =>
                        {
                            if (
                                args != null &&
                                args.Error != null
                            )
                            {
                                LastError =
                                    args.Error;
                            }
                        };

                    ConnectionFactory factory =
                        new ConnectionFactory();

                    _connection =
                        factory.CreateConnection(
                            options
                        );

                    ConnectedAt =
                        DateTime.Now;

                    LastError = null;
                }
                catch (Exception ex)
                {
                    _connection = null;

                    LastError =
                        ex.Message;
                }
            }
        }

        public static void Stop()
        {
            lock (_lock)
            {
                if (_connection != null)
                {
                    try
                    {
                        _connection.Close();
                    }
                    catch
                    {
                    }

                    _connection = null;
                }
            }
        }
    }
}
