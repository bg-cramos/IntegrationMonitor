using IntegrationMonitor.Models;

using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Net;
using System.Threading;

using Newtonsoft.Json.Linq;

// IMPORTANTE:
// Este namespace corresponde al paquete NATS.Client.
// Si Visual Studio marca error aquí,
// significa que todavía falta instalar NATS.Client.
using NATS.Client;

namespace IntegrationMonitor.Services
{
    public class MonitorService
    {
        // =====================================================
        // CONFIGURACION
        // =====================================================

        private string ObtenerConfiguracion(string key)
        {
            return ConfigurationManager.AppSettings[key];
        }


        // =====================================================
        // NATS
        // =====================================================

        private string NatsServerHost
        {
            get
            {
                string valor =
                    ObtenerConfiguracion(
                        "NatsServer"
                    );

                if (
                    string.IsNullOrWhiteSpace(
                        valor
                    )
                )
                {
                    return "127.0.0.1";
                }

                return valor.Trim();
            }
        }


        private int NatsServerPort
        {
            get
            {
                int puerto;

                if (
                    int.TryParse(
                        ObtenerConfiguracion(
                            "NatsPort"
                        ),
                        out puerto
                    )
                )
                {
                    return puerto;
                }

                return 4222;
            }
        }


        private int NatsMonitoringPort
        {
            get
            {
                int puerto;

                if (
                    int.TryParse(
                        ObtenerConfiguracion(
                            "NatsMonitoringPort"
                        ),
                        out puerto
                    )
                )
                {
                    return puerto;
                }

                return 8222;
            }
        }


        private string NatsMonitoringUrl
        {
            get
            {
                return
                    "http://"
                    + NatsServerHost
                    + ":"
                    + NatsMonitoringPort
                    + "/connz?subs=true";
            }
        }


        // =====================================================
        // MENSAJES NATS
        // =====================================================

        private class MensajeNats
        {
            public long Id { get; set; }

            public int Cid { get; set; }

            public string Subject { get; set; }

            public string Mensaje { get; set; }

            public DateTime Fecha { get; set; }
        }


        private static readonly object
            MensajesLock =
                new object();


        private static readonly Dictionary<
            string,
            MensajeNats
        >
            UltimosMensajes =
                new Dictionary<
                    string,
                    MensajeNats
                >(
                    StringComparer.OrdinalIgnoreCase
                );


        private static long
            SecuenciaMensaje = 0;

        // =====================================================
        // ESTADISTICAS DE ACTIVIDAD NATS
        // =====================================================

        private static long
            TotalRecibidos = 0;

        private static long
            TotalProcesados = 0;

        private static long
            TotalErrores = 0;

        private static DateTime?
            FechaUltimoMensaje = null;

        private static readonly object
            EstadisticasLock =
                new object();



        // =====================================================
        // SUSCRIPCIONES
        // =====================================================

        private static readonly object
            SuscripcionesLock =
                new object();


        private static IConnection
    ConexionMonitorNats = null;

        private static readonly object
            ConexionMonitorLock =
                new object();



        private static readonly Dictionary<
            string,
            IAsyncSubscription
        >
            SuscripcionesNats =
                new Dictionary<
                    string,
                    IAsyncSubscription
                >(
                    StringComparer.OrdinalIgnoreCase
                );


        // =====================================================
        // OBTENER CONEXION DEL MONITOR
        // =====================================================

        private IConnection ObtenerConexionMonitorNats(
     out string error
 )
        {
            error = "";

            lock (
                ConexionMonitorLock
            )
            {
                try
                {
                    // =================================================
                    // YA EXISTE UNA CONEXION ACTIVA
                    // =================================================

                    if (
                        ConexionMonitorNats != null
                        &&
                        ConexionMonitorNats.State
                            == ConnState.CONNECTED
                    )
                    {
                        return ConexionMonitorNats;
                    }


                    // =================================================
                    // CREAR CONEXION DEL MONITOR
                    // =================================================

                    ConnectionFactory factory =
                        new ConnectionFactory();


                    Options options =
                        ConnectionFactory.GetDefaultOptions();


                    options.Url =
                        "nats://"
                        + NatsServerHost
                        + ":"
                        + NatsServerPort;


                    ConexionMonitorNats =
                        factory.CreateConnection(
                            options
                        );


                    return ConexionMonitorNats;
                }
                catch (
                    Exception ex
                )
                {
                    error =
                        ex.Message;

                    ConexionMonitorNats =
                        null;

                    return null;
                }
            }
        }




        // =====================================================
        // SUSCRIBIRSE A SUBJECT
        // =====================================================

        public bool Suscribirse(
            int cid,
            string subject,
            out string error
        )
        {
            error = "";


            subject =
                (subject ?? "").Trim();


            if (
                string.IsNullOrWhiteSpace(
                    subject
                )
            )
            {
                error =
                    "El Subject está vacío.";

                return false;
            }


            lock (
                SuscripcionesLock
            )
            {
                // -------------------------------------------------
                // YA ESTÁ SUSCRIPTO
                // -------------------------------------------------

                if (
                    SuscripcionesNats.ContainsKey(
                        subject
                    )
                )
                {
                    return true;
                }


                // -------------------------------------------------
                // OBTENER CONEXION UNICA
                // -------------------------------------------------

                IConnection connection =
                    ObtenerConexionMonitorNats(
                        out error
                    );


                if (
                    connection == null
                )
                {
                    return false;
                }


                try
                {
                    IAsyncSubscription subscription =
                    connection.SubscribeAsync(
                        subject
                    );


                    System.Diagnostics.Debug.WriteLine(
                        "NATS MONITOR SUSCRIPTO - Subject: "
                        + subject
                    );


                    subscription.MessageHandler +=
                        delegate (
                            object sender,
                            MsgHandlerEventArgs args
                        )
                        {
                            System.Diagnostics.Debug.WriteLine(
                                "NATS MONITOR RECIBIO MENSAJE - Subject: "
                                + subject
                            );

                            ProcesarMensajeNats(
                                0,
                                subject,
                                args.Message
                            );
                        };


                    subscription.Start();


                    System.Diagnostics.Debug.WriteLine(
                        "NATS MONITOR START - Subject: "
                        + subject
                    );



                    SuscripcionesNats[
                        subject
                    ] =
                        subscription;


                    return true;
                }
                catch (
                    Exception ex
                )
                {
                    error =
                        ex.Message;

                    return false;
                }
            }
        }



        // =====================================================
        // RECIBIR MENSAJE
        // =====================================================

        private void ProcesarMensajeNats(
            int cid,
            string subject,
            Msg mensaje
        )
        {
            if (
                mensaje == null
            )
            {
                return;
            }


            // =================================================
            // MENSAJE RECIBIDO
            // =================================================

            Interlocked.Increment(
                ref TotalRecibidos
            );

            System.Diagnostics.Debug.WriteLine(
                "NATS RECIBIDO - TotalRecibidos: "
                + TotalRecibidos
            );



            try
            {
                string contenido =
                    mensaje.Data != null
                        ? System.Text.Encoding.UTF8.GetString(
                            mensaje.Data
                        )
                        : "";


                long id =
                    Interlocked.Increment(
                        ref SecuenciaMensaje
                    );


                var nuevoMensaje =
                    new MensajeNats
                    {
                        Id =
                            id,

                        Cid =
                            cid,

                        Subject =
                            subject,

                        Mensaje =
                            contenido,

                        Fecha =
                            DateTime.Now
                    };


                string clave =
                    ConstruirClave(
                        cid,
                        subject
                    );


                // =================================================
                // GUARDAR ULTIMO MENSAJE
                // =================================================

                lock (
                    MensajesLock
                )
                {
                    UltimosMensajes[
                        clave
                    ] =
                        nuevoMensaje;
                }


                // =================================================
                // ESTADISTICAS
                // =================================================

                lock (
                    EstadisticasLock
                )
                {
                    TotalProcesados++;

                    FechaUltimoMensaje =
                        nuevoMensaje.Fecha;
                    System.Diagnostics.Debug.WriteLine(
                        "NATS PROCESADO - TotalProcesados: "
                        + TotalProcesados
                    );

                }
            }
            catch
            {
                // =================================================
                // ERROR PROCESANDO MENSAJE
                // =================================================

                Interlocked.Increment(
                    ref TotalErrores
                );
            }
        }



        // =====================================================
        // OBTENER ULTIMO MENSAJE
        // =====================================================

        public object ObtenerUltimoMensaje(
            int cid,
            string subject,
            long ultimoMensajeId
        )
        {
            subject =
                (subject ?? "").Trim();


            if (
                string.IsNullOrWhiteSpace(
                    subject
                )
            )
            {
                return new
                {
                    conectado = false,

                    hayMensaje = false,

                    id = ultimoMensajeId,

                    mensaje = "",

                    error = "El Subject está vacío."
                };
            }


            string error;


            bool conectado =
                Suscribirse(
                    cid,
                    subject,
                    out error
                );


            string clave =
                ConstruirClave(
                    cid,
                    subject
                );


            lock (
                MensajesLock
            )
            {
                MensajeNats mensaje;


                if (
                    !UltimosMensajes.TryGetValue(
                        clave,
                        out mensaje
                    )
                )
                {
                    return new
                    {
                        conectado =
                            conectado,

                        hayMensaje =
                            false,

                        id =
                            ultimoMensajeId,

                        mensaje =
                            "",

                        error =
                            error
                    };
                }


                if (
                    mensaje.Id
                    <=
                    ultimoMensajeId
                )
                {
                    return new
                    {
                        conectado =
                            conectado,

                        hayMensaje =
                            false,

                        id =
                            ultimoMensajeId,

                        mensaje =
                            "",

                        error =
                            error
                    };
                }


                return new
                {
                    conectado =
                        conectado,

                    hayMensaje =
                        true,

                    id =
                        mensaje.Id,

                    mensaje =
                        mensaje.Mensaje,

                    error =
                        error
                };
            }
        }



        // =====================================================
        // CLAVE SUSCRIPCION
        // =====================================================

        private string ConstruirClave(
            int cid,
            string subject
        )
        {
            // Una sola suscripción por Subject.
            //
            // El CID identifica una conexión existente en NATS,
            // pero nuestro monitor crea su propia conexión.
            //
            // Por eso NO debe formar parte de la clave.
            return
                (subject ?? "").Trim();
        }

        // =====================================================
        // ESTADO GENERAL
        // =====================================================

        public IntegrationStatus ObtenerEstado()
        {
            var estado =
                new IntegrationStatus();

                       
            // -------------------------------------------------
            // SERVIDOR NATS
            // -------------------------------------------------

            estado.NatsServer =
                NatsServerHost
                + ":"
                + NatsServerPort;


            estado.UltimaActualizacion =
                DateTime.Now;


            // -------------------------------------------------
            // NATS
            // -------------------------------------------------

            estado.NatsConnected =
                VerificarNats();


            estado.NatsStatus =
                estado.NatsConnected
                    ? "CONNECTED"
                    : "DISCONNECTED";


            // -------------------------------------------------
            // SUBJECTS Y CONEXIONES NATS
            // -------------------------------------------------

            if (
    estado.NatsConnected
)
            {
                ObtenerSubjectsNats(
                    estado
                );

                // -------------------------------------------------
                // ACTIVAR MONITOREO DE LOS SUBJECTS
                // -------------------------------------------------

                foreach (
                    NatsSubjectStatus subjectStatus
                    in estado.Subjects
                )
                {
                    string error;

                    bool suscripto =
                        Suscribirse(
                            0,
                            subjectStatus.Subject,
                            out error
                        );

                    System.Diagnostics.Debug.WriteLine(
                        "NATS SUSCRIPCION - Subject: "
                        + subjectStatus.Subject
                        + " | OK: "
                        + suscripto
                        + " | Error: "
                        + error
                    );

                    if (!suscripto)
                    {
                        estado.Eventos.Add(
                            new IntegrationEvent
                            {
                                Fecha =
                                    DateTime.Now,

                                Tipo =
                                    "NATS_ERROR",

                                Mensaje =
                                    "No se pudo suscribir al Subject: "
                                    + subjectStatus.Subject
                                    + " | Error: "
                                    + error
                            }
                        );
                    }
                }


                //    // -------------------------------------------------
                //    // ACTIVAR MONITOREO DE LOS SUBJECTS ORIGINALES
                //    // -------------------------------------------------

                //                foreach (
                //    NatsConnectionStatus connection
                //    in estado.NatsConnections
                //)
                //    {
                //        foreach (
                //            string subject
                //            in connection.SubscriptionsList
                //        )
                //        {
                //            string error;

                //            bool suscripto =
                //                Suscribirse(
                //                    connection.Cid,
                //                    subject,
                //                    out error
                //                );

                //            estado.Eventos.Add(
                //                new IntegrationEvent
                //                {
                //                    Fecha =
                //                        DateTime.Now,

                //                    Tipo =
                //                        suscripto
                //                            ? "NATS_SUBSCRIBED"
                //                            : "NATS_ERROR",

                //                    Mensaje =
                //                        "CID: "
                //                        + connection.Cid
                //                        + " | Subject: "
                //                        + subject
                //                        + " | OK: "
                //                        + suscripto
                //                        + " | Error: "
                //                        + error
                //                }
                //            );
                //        }
                //    }

                }



                // -------------------------------------------------
                // ORDENAR EVENTOS
                // -------------------------------------------------

                estado.Eventos =
                estado.Eventos
                    .OrderByDescending(
                        x => x.Fecha
                    )
                    .Take(10)
                    .ToList();


            return estado;
        }


        // =====================================================
        // VERIFICAR NATS
        // =====================================================

        private bool VerificarNats()
        {
            try
            {
                using (
                    var client =
                        new System.Net.Sockets.TcpClient()
                )
                {
                    var resultado =
                        client
                            .ConnectAsync(
                                NatsServerHost,
                                NatsServerPort
                            )
                            .Wait(
                                TimeSpan.FromSeconds(2)
                            );

                    return resultado;
                }
            }
            catch
            {
                return false;
            }
        }


        // =====================================================
        // OBTENER SUBJECTS NATS
        // =====================================================

        private void ObtenerSubjectsNats(
            IntegrationStatus estado
        )
        {
            try
            {
                string json;


                using (
                    var client =
                        new WebClient()
                )
                {
                    client.Encoding =
                        System.Text.Encoding.UTF8;


                    json =
                        client.DownloadString(
                            NatsMonitoringUrl
                        );
                }


                if (
                    string.IsNullOrWhiteSpace(
                        json
                    )
                )
                {
                    return;
                }


                JObject root =
                    JObject.Parse(
                        json
                    );


                JArray connections =
                    root["connections"]
                        as JArray;


                if (
                    connections == null
                )
                {
                    return;
                }


                foreach (
                    JToken connectionToken
                    in connections
                )
                {
                    var connection =
                        new NatsConnectionStatus();


                    connection.Cid =
                        connectionToken["cid"] != null
                            ? connectionToken["cid"].Value<int>()
                            : 0;


                    connection.Kind =
                        connectionToken["kind"] != null
                            ? connectionToken["kind"].ToString()
                            : "";


                    connection.Type =
                        connectionToken["type"] != null
                            ? connectionToken["type"].ToString()
                            : "";


                    connection.Ip =
                        connectionToken["ip"] != null
                            ? connectionToken["ip"].ToString()
                            : "";


                    connection.Port =
                        connectionToken["port"] != null
                            ? connectionToken["port"].Value<int>()
                            : 0;


                    connection.Start =
                        connectionToken["start"] != null
                            ? connectionToken["start"].ToString()
                            : "";


                    connection.LastActivity =
                        connectionToken["last_activity"] != null
                            ? connectionToken["last_activity"].ToString()
                            : "";


                    connection.Uptime =
                        connectionToken["uptime"] != null
                            ? connectionToken["uptime"].ToString()
                            : "";


                    connection.Subscriptions =
                        connectionToken["subscriptions"] != null
                            ? connectionToken["subscriptions"].Value<int>()
                            : 0;


                    connection.Lang =
                        connectionToken["lang"] != null
                            ? connectionToken["lang"].ToString()
                            : "";


                    connection.Version =
                        connectionToken["version"] != null
                            ? connectionToken["version"].ToString()
                            : "";


                    JArray subscriptions =
                        connectionToken[
                            "subscriptions_list"
                        ] as JArray;


                    if (
                        subscriptions != null
                    )
                    {
                        foreach (
                            JToken subscription
                            in subscriptions
                        )
                        {
                            string subject =
                                subscription.ToString();


                            if (
                                string.IsNullOrWhiteSpace(
                                    subject
                                )
                            )
                            {
                                continue;
                            }


                            connection
                                .SubscriptionsList
                                .Add(
                                    subject
                                );
                        }
                    }


                    estado.NatsConnections.Add(
                        connection
                    );


                    foreach (
                        string subject
                        in connection.SubscriptionsList
                    )
                    {
                        var existente =
                            estado.Subjects
                                .FirstOrDefault(
                                    x =>
                                        x.Subject.Equals(
                                            subject,
                                            StringComparison
                                                .OrdinalIgnoreCase
                                        )
                                );


                        if (
                            existente == null
                        )
                        {
                            existente =
                                new NatsSubjectStatus
                                {
                                    Subject =
                                        subject,

                                    Connections =
                                        0
                                };


                            estado.Subjects.Add(
                                existente
                            );
                        }


                        existente.Connections++;


                        existente.Clients.Add(
                            connection
                        );
                    }
                }


                estado.Subjects =
                    estado.Subjects
                        .OrderBy(
                            x => x.Subject
                        )
                        .ToList();
            }
            catch (
                Exception ex
            )
            {
                estado.Eventos.Add(
                    new IntegrationEvent
                    {
                        Fecha =
                            DateTime.Now,

                        Tipo =
                            "WARNING",

                        Mensaje =
                            "No se pudo consultar los subjects NATS: "
                            + ex.Message
                    }
                );
            }
        }
    }
}
