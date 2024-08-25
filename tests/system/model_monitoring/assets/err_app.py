import mlrun.model_monitoring.applications.context as mm_context
import mlrun.model_monitoring.applications.results as mm_results
from mlrun.model_monitoring.applications import (
    ModelMonitoringApplicationBaseV2,
)


class ErrApp(ModelMonitoringApplicationBaseV2):
    NAME = "err-app"

    def do_tracking(
        self,
        monitoring_context: mm_context.MonitoringApplicationContext,
    ) -> list[mm_results.ModelMonitoringApplicationResult]:
        monitoring_context.logger.info("Running demo app")
        raise ValueError(f" This is an ERROR from {self.NAME} app!")
