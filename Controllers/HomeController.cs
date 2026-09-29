using System.Web.Mvc;
using IntegrationMonitor.Models;
using IntegrationMonitor.Services;

namespace IntegrationMonitor.Controllers
{
    public class HomeController : Controller
    {
        private readonly MonitorService _monitorService =
        new MonitorService();

        // =====================================================
        // INICIO
        // =====================================================

        public ActionResult Index()
        {
            var estado =
                _monitorService.ObtenerEstado();

            return View(estado);
        }


        // =====================================================
        // ESTADO
        // =====================================================

        [HttpGet]
        public JsonResult Estado()
        {
            var estado =
                _monitorService.ObtenerEstado();

            return Json(
                estado,
                JsonRequestBehavior.AllowGet
            );
        }


        // =====================================================
        // LOG
        // =====================================================

        [HttpGet]
        public ActionResult Logs(
            int cid,
            string subject,
            string consumer)
        {
            // -------------------------------------------------
            // NORMALIZAR DATOS
            // -------------------------------------------------

            subject =
                (subject ?? "").Trim();

            consumer =
                (consumer ?? "").Trim();


            // -------------------------------------------------
            // OBTENER LOG
            // -------------------------------------------------

            var logInfo =
                _monitorService.ObtenerLog(
                    cid,
                    subject,
                    consumer
                );


            // -------------------------------------------------
            // DETERMINAR NOMBRE DEL CONSUMER
            // -------------------------------------------------

            string consumerName =
                logInfo.ConsumerName;


            if (
                string.IsNullOrWhiteSpace(
                    consumerName
                )
            )
            {
                consumerName =
                    consumer;
            }


            // -------------------------------------------------
            // MODEL
            // -------------------------------------------------

            var model =
                new LogViewModel
                {
                    Cid =
                        cid,

                    Subject =
                        subject,

                    Consumer =
                        logInfo.Consumer,

                    ConsumerName =
                        consumerName,

                    LogFile =
                        logInfo.LogFile,

                    LineasLog =
                        logInfo.LineasLog
                };


            return View(model);
        }


        // =====================================================
        // LIMPIAR LOG
        // =====================================================

        [HttpPost]
        public JsonResult LimpiarLog(
            int cid,
            string subject,
            string consumer)
        {
            // -------------------------------------------------
            // NORMALIZAR DATOS
            // -------------------------------------------------

            subject =
                (subject ?? "").Trim();

            consumer =
                (consumer ?? "").Trim();


            // -------------------------------------------------
            // VALIDAR CONSUMER
            // -------------------------------------------------

            if (
                string.IsNullOrWhiteSpace(
                    consumer
                )
            )
            {
                return Json(
                    new
                    {
                        ok = false,

                        mensaje =
                            "No se recibió el Consumer del log."
                    }
                );
            }


            // -------------------------------------------------
            // LIMPIAR LOG
            // -------------------------------------------------

            var resultado =
                _monitorService.LimpiarLog(
                    cid,
                    subject,
                    consumer
                );


            return Json(
                resultado
            );
        }
    }


}