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
        // MONITOR NATS
        // =====================================================

        [HttpGet]
        public ActionResult Logs(
            int cid,
            string subject)
        {
            subject =
                (subject ?? "").Trim();

            var model =
                new LogViewModel
                {
                    Cid =
                        cid,

                    Subject =
                        subject
                };

            return View(model);
        }


        // =====================================================
        // MENSAJE NATS
        // =====================================================

        [HttpGet]
        public JsonResult MensajeNats(
            int cid,
            string subject,
            long ultimoMensajeId = 0)
        {
            var resultado =
                _monitorService.ObtenerUltimoMensaje(
                    cid,
                    subject,
                    ultimoMensajeId
                );

            return Json(
                resultado,
                JsonRequestBehavior.AllowGet
            );
        }
    }
}
