using IntegrationMonitor.Models;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text.RegularExpressions;
using Newtonsoft.Json.Linq;

namespace IntegrationMonitor.Services
{
    public class MonitorService
    {
        private const string NatsPythonLog =
            @"D:\Mailchimp\AltaUserDemo\NatsPython_Log.txt";

        private const string NatsServicioLog =
            @"D:\Mailchimp\AltaUserDemo\nats_servicio.log";

        private const string GitHubLog =
            @"D:\IGGLOBAL Online\nats_github_push_global_update_respuesta_evento.log";

        private const string NatsMonitoringUrl =
            "http://127.0.0.1:8222/connz?subs=true";


        // =====================================================
        // ESTADO GENERAL
        // =====================================================

        public IntegrationStatus ObtenerEstado()
        {
            var estado =
                new IntegrationStatus();

            estado.NatsServer =
                "localhost:4222";

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

            if (estado.NatsConnected)
            {
                ObtenerSubjectsNats(
                    estado
                );
            }


            // -------------------------------------------------
            // LOG NATS PYTHON
            // -------------------------------------------------

            AnalizarNatsPython(
                estado
            );


            // -------------------------------------------------
            // LOG GITHUB
            // -------------------------------------------------

            AnalizarGitHub(
                estado
            );


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
                                "127.0.0.1",
                                4222
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


        private void ObtenerSubjectsNats(
    IntegrationStatus estado
)
        {
            try
            {
                string json;

                using (var client = new WebClient())
                {
                    client.Encoding =
                        System.Text.Encoding.UTF8;

                    json =
                        client.DownloadString(
                            NatsMonitoringUrl
                        );
                }

                if (string.IsNullOrWhiteSpace(json))
                {
                    return;
                }

                JObject root =
                    JObject.Parse(json);

                JArray connections =
                    root["connections"] as JArray;

                if (connections == null)
                {
                    return;
                }

                foreach (JToken connectionToken in connections)
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
                        connectionToken["subscriptions_list"]
                            as JArray;

                    if (subscriptions != null)
                    {
                        foreach (JToken subscription in subscriptions)
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
                                .Add(subject);
                        }
                    }

                    // ---------------------------------------------
                    // GUARDAR CONEXIÓN
                    // ---------------------------------------------

                    estado.NatsConnections.Add(
                        connection
                    );

                    // ---------------------------------------------
                    // CREAR LISTA DE SUBJECTS
                    // ---------------------------------------------

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

                        if (existente == null)
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

                // ---------------------------------------------
                // ORDENAR SUBJECTS
                // ---------------------------------------------

                estado.Subjects =
                    estado.Subjects
                        .OrderBy(
                            x => x.Subject
                        )
                        .ToList();
            }
            catch (Exception ex)
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

        // =====================================================
        // CONVERTIR CONEXION
        // =====================================================

        private NatsConnectionStatus ConvertirConexion(
            Dictionary<string, object> data
        )
        {
            var resultado =
                new NatsConnectionStatus();


            resultado.Cid =
                ObtenerInt(
                    data,
                    "cid"
                );


            resultado.Kind =
                ObtenerString(
                    data,
                    "kind"
                );


            resultado.Type =
                ObtenerString(
                    data,
                    "type"
                );


            resultado.Ip =
                ObtenerString(
                    data,
                    "ip"
                );


            resultado.Port =
                ObtenerInt(
                    data,
                    "port"
                );


            resultado.Start =
                ObtenerString(
                    data,
                    "start"
                );


            resultado.LastActivity =
                ObtenerString(
                    data,
                    "last_activity"
                );


            resultado.Uptime =
                ObtenerString(
                    data,
                    "uptime"
                );


            resultado.Subscriptions =
                ObtenerInt(
                    data,
                    "subscriptions"
                );


            resultado.Lang =
                ObtenerString(
                    data,
                    "lang"
                );


            resultado.Version =
                ObtenerString(
                    data,
                    "version"
                );


            if (
                data.ContainsKey(
                    "subscriptions_list"
                )
            )
            {
                var lista =
                    data["subscriptions_list"]
                        as object[];


                if (lista != null)
                {
                    foreach (
                        var item
                        in lista
                    )
                    {
                        if (
                            item != null
                        )
                        {
                            resultado
                                .SubscriptionsList
                                .Add(
                                    item.ToString()
                                );
                        }
                    }
                }
            }


            return resultado;
        }


        // =====================================================
        // HELPERS JSON
        // =====================================================

        private string ObtenerString(
            Dictionary<string, object> data,
            string key
        )
        {
            if (
                data == null ||
                !data.ContainsKey(key) ||
                data[key] == null
            )
            {
                return "";
            }

            return data[key].ToString();
        }


        private int ObtenerInt(
            Dictionary<string, object> data,
            string key
        )
        {
            if (
                data == null ||
                !data.ContainsKey(key) ||
                data[key] == null
            )
            {
                return 0;
            }

            int resultado;

            if (
                int.TryParse(
                    data[key].ToString(),
                    out resultado
                )
            )
            {
                return resultado;
            }

            return 0;
        }


        // =====================================================
        // NATS PYTHON
        // =====================================================

        private void AnalizarNatsPython(
            IntegrationStatus estado
        )
        {
            if (
                !File.Exists(
                    NatsPythonLog
                )
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
                            "No se encontró NatsPython_Log.txt"
                    }
                );

                return;
            }


            string[] lineas;


            try
            {
                lineas =
                    File.ReadAllLines(
                        NatsPythonLog
                    );
            }
            catch (Exception ex)
            {
                estado.Eventos.Add(
                    new IntegrationEvent
                    {
                        Fecha =
                            DateTime.Now,

                        Tipo =
                            "ERROR",

                        Mensaje =
                            "No se pudo leer NatsPython_Log.txt: "
                            + ex.Message
                    }
                );

                return;
            }


            foreach (
                string linea
                in lineas
            )
            {
                if (
                    linea.Contains(
                        "Mensaje recibido desde NATS"
                    )
                )
                {
                    estado.Recibidos++;


                    AgregarEvento(
                        estado,
                        linea,
                        "INFO",
                        "NATS Python recibió un mensaje"
                    );
                }


                if (
                    linea.Contains(
                        "Procesamiento del mensaje terminado correctamente"
                    )
                )
                {
                    estado.Procesados++;


                    AgregarEvento(
                        estado,
                        linea,
                        "OK",
                        "NATS Python procesó un mensaje"
                    );
                }


                if (
                    linea.Contains(
                        "[ERROR]"
                    )
                )
                {
                    estado.Errores++;


                    AgregarEvento(
                        estado,
                        linea,
                        "ERROR",
                        ExtraerMensaje(
                            linea
                        )
                    );
                }


                if (
                    linea.Contains(
                        "NATS conectado"
                    )
                )
                {
                    AgregarEvento(
                        estado,
                        linea,
                        "OK",
                        "NATS Python conectado"
                    );
                }


                if (
                    linea.Contains(
                        "NATS reconectado"
                    )
                )
                {
                    AgregarEvento(
                        estado,
                        linea,
                        "WARNING",
                        "NATS Python reconectado"
                    );
                }


                if (
                    linea.Contains(
                        "NATS desconectado"
                    )
                )
                {
                    AgregarEvento(
                        estado,
                        linea,
                        "ERROR",
                        "NATS Python desconectado"
                    );
                }
            }
        }


        // =====================================================
        // GITHUB
        // =====================================================

        private void AnalizarGitHub(
            IntegrationStatus estado
        )
        {
            if (
                !File.Exists(
                    GitHubLog
                )
            )
            {
                return;
            }


            string[] lineas;


            try
            {
                lineas =
                    File.ReadAllLines(
                        GitHubLog
                    );
            }
            catch
            {
                return;
            }


            foreach (
                string linea
                in lineas
            )
            {
                if (
                    linea.Contains(
                        "[ERROR]"
                    )
                )
                {
                    AgregarEvento(
                        estado,
                        linea,
                        "ERROR",
                        ExtraerMensaje(
                            linea
                        )
                    );
                }


                if (
                    linea.Contains(
                        "Procesamiento"
                    )
                    ||
                    linea.Contains(
                        "procesado"
                    )
                )
                {
                    AgregarEvento(
                        estado,
                        linea,
                        "OK",
                        ExtraerMensaje(
                            linea
                        )
                    );
                }


                if (
                    linea.Contains(
                        "NATS conectado"
                    )
                )
                {
                    AgregarEvento(
                        estado,
                        linea,
                        "OK",
                        "GitHub Push conectado a NATS"
                    );
                }


                if (
                    linea.Contains(
                        "NATS reconectado"
                    )
                )
                {
                    AgregarEvento(
                        estado,
                        linea,
                        "WARNING",
                        "GitHub Push reconectado a NATS"
                    );
                }
            }
        }


        // =====================================================
        // EVENTO
        // =====================================================

        private void AgregarEvento(
            IntegrationStatus estado,
            string linea,
            string tipo,
            string mensaje
        )
        {
            DateTime fecha;


            if (
                !TryObtenerFecha(
                    linea,
                    out fecha
                )
            )
            {
                fecha =
                    DateTime.Now;
            }


            estado.Eventos.Add(
                new IntegrationEvent
                {
                    Fecha =
                        fecha,

                    Tipo =
                        tipo,

                    Mensaje =
                        mensaje
                }
            );


            if (
                mensaje.Contains(
                    "recibió un mensaje"
                )
            )
            {
                estado.UltimoMensaje =
                    fecha;
            }
        }


        // =====================================================
        // FECHA
        // =====================================================

        private bool TryObtenerFecha(
            string linea,
            out DateTime fecha
        )
        {
            fecha =
                DateTime.MinValue;


            if (
                string.IsNullOrWhiteSpace(
                    linea
                )
            )
            {
                return false;
            }


            string patron =
                @"^(\d{2}/\d{2}/\d{4})\s+(\d{2}:\d{2}:\d{2})";


            Match match =
                Regex.Match(
                    linea,
                    patron
                );


            if (
                !match.Success
            )
            {
                return false;
            }


            return DateTime.TryParse(
                match.Groups[1].Value
                + " "
                + match.Groups[2].Value,
                out fecha
            );
        }


        // =====================================================
        // MENSAJE
        // =====================================================

        private string ExtraerMensaje(
            string linea
        )
        {
            Match match =
                Regex.Match(
                    linea,
                    @"\[(.*?)\]\s+(.*)$"
                );


            if (
                match.Success
            )
            {
                return match.Groups[2].Value;
            }


            return linea;
        }
    }
}
