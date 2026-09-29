using System;
using System.Collections.Generic;

namespace IntegrationMonitor.Models
{
    public class IntegrationStatus
    {
        public string NatsServer { get; set; }

        public bool NatsConnected { get; set; }

        public string NatsStatus { get; set; }

        public string Subject { get; set; }

        public DateTime UltimaActualizacion { get; set; }

        public int Recibidos { get; set; }

        public int Procesados { get; set; }

        public int Errores { get; set; }

        public DateTime? UltimoMensaje { get; set; }

        public List<IntegrationEvent> Eventos { get; set; }

        public List<NatsSubjectStatus> Subjects { get; set; }

        public List<NatsConnectionStatus> NatsConnections { get; set; }

        public IntegrationStatus()
        {
            Eventos = new List<IntegrationEvent>();

            Subjects = new List<NatsSubjectStatus>();

            NatsConnections =
                new List<NatsConnectionStatus>();
        }
    }


    // =====================================================
    // EVENTO DEL MONITOR
    // =====================================================

    public class IntegrationEvent
    {
        public DateTime Fecha { get; set; }

        public string Tipo { get; set; }

        public string Mensaje { get; set; }
    }


    // =====================================================
    // SUBJECT NATS
    // =====================================================

    public class NatsSubjectStatus
    {
        public string Subject { get; set; }

        public int Connections { get; set; }

        public List<NatsConnectionStatus> Clients { get; set; }

        public NatsSubjectStatus()
        {
            Clients =
                new List<NatsConnectionStatus>();
        }
    }


    // =====================================================
    // CONEXION NATS
    // =====================================================

    public class NatsConnectionStatus
    {
        public int Cid { get; set; }

        public string Kind { get; set; }

        public string Type { get; set; }

        public string Ip { get; set; }

        public int Port { get; set; }

        public string Start { get; set; }

        public string LastActivity { get; set; }

        public string Uptime { get; set; }

        public int Subscriptions { get; set; }

        public string Lang { get; set; }

        public string Version { get; set; }

        public List<string> SubscriptionsList { get; set; }

        public NatsConnectionStatus()
        {
            SubscriptionsList =
                new List<string>();
        }
    }
}