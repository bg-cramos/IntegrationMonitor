using IntegrationMonitor.Models;

using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Net;
using System.Text.RegularExpressions;

using Newtonsoft.Json.Linq;

namespace IntegrationMonitor.Services
{
    public class MonitorService
    {
        public class LogInfo
        {
            public string Consumer { get; set; }

            public string ConsumerName { get; set; }

            public string LogFile { get; set; }

            public List<string> LineasLog { get; set; }
        }

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

                if (string.IsNullOrWhiteSpace(valor))
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
        // RUTAS DE LOG
        // =====================================================

        private string NatsPythonLog
        {
            get
            {
                return ObtenerRutaLogConfigurada(
                    "Log.globalcontactform.natspython"
                );
            }
        }

        private string ContactFormPbpLog
        {
            get
            {
                return ObtenerRutaLogConfigurada(
                    "Log.globalcontactform.contactformpbp"
                );
            }
        }

        private string GitHubLog
        {
            get
            {
                return ObtenerRutaLogConfigurada(
                    "Log.github"
                );
            }
        }

        // =====================================================
        // CONSTRUIR RUTA DE LOG REMOTA
        // =====================================================

        private string ObtenerRutaLogConfigurada(
            string prefijo
        )
        {
            string server =
                ObtenerConfiguracion(
                    prefijo + ".Server"
                );

            string share =
                ObtenerConfiguracion(
                    prefijo + ".Share"
                );

            string path =
                ObtenerConfiguracion(
                    prefijo + ".Path"
                );

            // -------------------------------------------------
            // VALIDAR
            // -------------------------------------------------

            if (
                string.IsNullOrWhiteSpace(server)
                ||
                string.IsNullOrWhiteSpace(share)
                ||
                string.IsNullOrWhiteSpace(path)
            )
            {
                return null;
            }

            server =
                server.Trim();

            share =
                share.Trim();

            path =
                path.Trim();

            // -------------------------------------------------
            // NORMALIZAR
            // -------------------------------------------------

            server =
                server
                    .TrimStart('\\')
                    .TrimEnd('\\');

            share =
                share
                    .TrimStart('\\')
                    .TrimEnd('\\');

            path =
                path
                    .TrimStart('\\');

            // -------------------------------------------------
            // RUTA UNC
            // -------------------------------------------------

            return
                @"\\"
                + server
                + @"\"
                + share
                + @"\"
                + path;
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
        // OBTENER LOG
        // =====================================================

        public LogInfo ObtenerLog(
            int cid,
            string subject,
            string consumer)
        {
            var resultado =
                new LogInfo
                {
                    Consumer =
                        consumer,

                    ConsumerName =
                        consumer,

                    LogFile =
                        "",

                    LineasLog =
                        new List<string>()
                };

            // -------------------------------------------------
            // VALIDAR DATOS
            // -------------------------------------------------

            if (
                string.IsNullOrWhiteSpace(
                    subject
                )
            )
            {
                resultado.LineasLog.Add(
                    "No se recibió ningún Subject."
                );

                return resultado;
            }

            // -------------------------------------------------
            // OBTENER RUTA
            // -------------------------------------------------

            string rutaLog =
                ObtenerRutaLog(
                    subject,
                    consumer
                );

            if (
                string.IsNullOrWhiteSpace(
                    rutaLog
                )
            )
            {
                resultado.LineasLog.Add(
                    "No existe un archivo de log asociado al Consumer '"
                    + consumer
                    + "' y Subject '"
                    + subject
                    + "'."
                );

                return resultado;
            }

            resultado.LogFile =
                rutaLog;

            // -------------------------------------------------
            // VALIDAR ARCHIVO
            // -------------------------------------------------

            if (
                !File.Exists(
                    rutaLog
                )
            )
            {
                resultado.LineasLog.Add(
                    "No se encontró el archivo de log:"
                );

                resultado.LineasLog.Add(
                    rutaLog
                );

                return resultado;
            }

            // -------------------------------------------------
            // LEER LOG
            // -------------------------------------------------

            try
            {
                string[] lineas =
                    File.ReadAllLines(
                        rutaLog
                    );

                if (
                    lineas.Length == 0
                )
                {
                    resultado.LineasLog.Add(
                        "El archivo de log está vacío."
                    );

                    return resultado;
                }

                resultado.LineasLog =
                    lineas
                        .Reverse()
                        .ToList();
            }
            catch (
                Exception ex
            )
            {
                resultado.LineasLog.Add(
                    "ERROR leyendo el archivo de log:"
                );

                resultado.LineasLog.Add(
                    ex.Message
                );
            }

            return resultado;
        }

        // =====================================================
        // DETERMINAR RUTA SEGUN SUBJECT / CONSUMER
        // =====================================================

        private string ObtenerRutaLog(
            string subject,
            string consumer)
        {
            string subjectNormalizado =
                string.IsNullOrWhiteSpace(
                    subject
                )
                    ? ""
                    : subject.Trim();

            string consumerNormalizado =
                string.IsNullOrWhiteSpace(
                    consumer
                )
                    ? ""
                    : consumer.Trim();

            // =================================================
            // GITHUB
            // =================================================

            if (
                string.Equals(
                    consumerNormalizado,
                    "github",
                    StringComparison.OrdinalIgnoreCase
                )
                ||
                string.Equals(
                    subjectNormalizado,
                    "github",
                    StringComparison.OrdinalIgnoreCase
                )
            )
            {
                return GitHubLog;
            }

            // =================================================
            // GLOBALCONTACTFORM
            // =================================================

            if (
                string.Equals(
                    subjectNormalizado,
                    "globalcontactform",
                    StringComparison.OrdinalIgnoreCase
                )
            )
            {
                // ---------------------------------------------
                // NATS PYTHON
                // ---------------------------------------------

                if (
                    string.Equals(
                        consumerNormalizado,
                        "natspython",
                        StringComparison.OrdinalIgnoreCase
                    )
                    ||
                    string.Equals(
                        consumerNormalizado,
                        "nats_python",
                        StringComparison.OrdinalIgnoreCase
                    )
                    ||
                    string.Equals(
                        consumerNormalizado,
                        "python",
                        StringComparison.OrdinalIgnoreCase
                    )
                )
                {
                    return NatsPythonLog;
                }

                // ---------------------------------------------
                // CONTACTFORMPBP
                // ---------------------------------------------

                if (
                    string.Equals(
                        consumerNormalizado,
                        "contactformpbp",
                        StringComparison.OrdinalIgnoreCase
                    )
                    ||
                    string.Equals(
                        consumerNormalizado,
                        "contactform_pbp",
                        StringComparison.OrdinalIgnoreCase
                    )
                    ||
                    string.Equals(
                        consumerNormalizado,
                        "mailchimpcontactformpbp",
                        StringComparison.OrdinalIgnoreCase
                    )
                )
                {
                    return ContactFormPbpLog;
                }
            }

            return null;
        }

        // =====================================================
        // LIMPIAR LOG
        // =====================================================

        public object LimpiarLog(
            int cid,
            string subject,
            string consumer)
        {
            string rutaLog =
                ObtenerRutaLog(
                    subject,
                    consumer
                );

            // -------------------------------------------------
            // VALIDAR RUTA
            // -------------------------------------------------

            if (
                string.IsNullOrWhiteSpace(
                    rutaLog
                )
            )
            {
                return new
                {
                    ok = false,

                    mensaje =
                        "No existe un archivo de log asociado al Consumer '"
                        + consumer
                        + "' y Subject '"
                        + subject
                        + "'."
                };
            }

            // -------------------------------------------------
            // VALIDAR EXISTENCIA
            // -------------------------------------------------

            if (
                !File.Exists(
                    rutaLog
                )
            )
            {
                return new
                {
                    ok = false,

                    mensaje =
                        "No se encontró el archivo de log: "
                        + rutaLog
                };
            }

            // -------------------------------------------------
            // LIMPIAR
            // -------------------------------------------------

            try
            {
                File.WriteAllText(
                    rutaLog,
                    string.Empty
                );

                return new
                {
                    ok = true,

                    mensaje =
                        "Log limpiado correctamente.",

                    archivo =
                        rutaLog
                };
            }
            catch (
                Exception ex
            )
            {
                return new
                {
                    ok = false,

                    mensaje =
                        "No se pudo limpiar el log: "
                        + ex.Message,

                    archivo =
                        rutaLog
                };
            }
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

                if (
                    lista != null
                )
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
                data == null
                ||
                !data.ContainsKey(
                    key
                )
                ||
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
                data == null
                ||
                !data.ContainsKey(
                    key
                )
                ||
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
                            "No se encontró el log de NATS Python: "
                            + NatsPythonLog
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
                            "ERROR",

                        Mensaje =
                            "No se pudo leer el log de NATS Python: "
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
                estado.Eventos.Add(
                    new IntegrationEvent
                    {
                        Fecha =
                            DateTime.Now,

                        Tipo =
                            "WARNING",

                        Mensaje =
                            "No se encontró el log de GitHub: "
                            + GitHubLog
                    }
                );

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
                            "ERROR",

                        Mensaje =
                            "No se pudo leer el log de GitHub: "
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
        // AGREGAR EVENTO
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
        // EXTRAER MENSAJE
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
