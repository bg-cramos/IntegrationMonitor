using System.Web.Mvc;
using IntegrationMonitor.Services;

namespace IntegrationMonitor.Controllers
{
    public class HomeController : Controller
    {
        private readonly MonitorService _monitorService =
            new MonitorService();

        public ActionResult Index()
        {
            var estado =
                _monitorService.ObtenerEstado();

            return View(estado);
        }

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
    }
}
